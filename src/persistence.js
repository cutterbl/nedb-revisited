import { join, dirname } from 'path';
import mkdirp from 'mkdirp';
import { serialize, deserialize } from './model';
import storage from './storage';
import { uid } from './customUtils';
import Index from './indexes';

export default class Persistence {
  // eslint-disable-next-line complexity
  constructor({
    db,
    nodeWebkitAppName,
    afterSerialization,
    beforeSerialization,
    corruptAlertThreshold = 0.1
  } = {}) {
    this.db = db;
    this.nodeWebkitAppName = nodeWebkitAppName;
    this.afterSerialization = afterSerialization;
    this.beforeDeserialization = beforeSerialization;
    this.corruptAlertThreshold = corruptAlertThreshold;
    this.inMemoryOnly = db.inMemoryOnly;
    this.filename = db.fileName;

    if (
      !this.inMemoryOnly &&
      this.filename &&
      this.filename.charAt(this.filename.length - 1) === '~'
    ) {
      throw new Error(
        "The datafile name can't end with a ~, which is reserved for crash safe backup files"
      );
    }

    // After serialization and before deserialization hooks with some basic sanity checks
    if (this.afterSerialization && !this.beforeDeserialization) {
      throw new Error(
        'Serialization hook defined but deserialization hook undefined, cautiously refusing to start NeDB to prevent dataloss'
      );
    }
    if (!this.afterSerialization && this.beforeDeserialization) {
      throw new Error(
        'Serialization hook undefined but deserialization hook defined, cautiously refusing to start NeDB to prevent dataloss'
      );
    }

    for (let i = 1; i < 30; i = i + 1) {
      for (let j = 0; j < 10; j = j + 1) {
        const randomString = uid(i);
        // eslint-disable-next-line max-depth
        if (this.beforeDeserialization(this.afterSerialization(randomString)) !== randomString) {
          throw new Error(
            'beforeDeserialization is not the reverse of afterSerialization, cautiously refusing to start NeDB to prevent dataloss'
          );
        }
      }
    }

    // For NW apps, store data in the same directory where NW stores application data
    if (this.filename && this.nodeWebkitAppName) {
      /* eslint-disable no-console */
      console.log('==================================================================');
      console.log('WARNING: The nodeWebkitAppName option is deprecated');
      console.log('To get the path to the directory where Node Webkit stores the data');
      console.log('for your app, use the internal nw.gui module like this');
      console.log("require('nw.gui').App.dataPath");
      console.log('See https://github.com/rogerwang/node-webkit/issues/500');
      console.log('==================================================================');
      /* eslint-enable no-console */
      this.filename = Persistence.getNWAppFilename(this.nodeWebkitAppName, this.filename);
    }
  }

  /**
   * Check if a directory exists and create it on the fly if it is not the case
   * @param {String} dir
   * @returns {Promise<any>}
   */
  static ensureDirectoryExists(dir) {
    return new Promise((resolve, reject) => {
      mkdirp(dir, function(err) {
        if (err) {
          reject(err);
          return;
        }
        resolve();
      });
    });
  }

  /**
   * Gets the NW app filename
   * @param appName {String}
   * @param relativeFilename {String}
   * @returns {String} Fully qualified app path to relativeFilename
   */
  static getNWAppFilename(appName, relativeFilename) {
    let home;

    switch (process.platform) {
      case 'win32':
      case 'win64':
        home = process.env.LOCALAPPDATA || process.env.APPDATA;
        if (!home) {
          throw new Error("Couldn't find the base application data folder");
        }
        home = join(home, appName);
        break;
      case 'darwin':
        home = process.env.HOME;
        if (!home) {
          throw new Error("Couldn't find the base application data directory");
        }
        home = join(home, 'Library', 'Application Support', appName);
        break;
      case 'linux':
        home = process.env.HOME;
        if (!home) {
          throw new Error("Couldn't find the base application data directory");
        }
        home = join(home, '.config', appName);
        break;
      default:
        throw new Error("Can't use the Node Webkit relative path for platform " + process.platform);
        break;
    }

    return join(home, 'nedb-data', relativeFilename);
  }

  /**
   * Persist cached database
   * This serves as a compaction function since the cache always contains only the number of documents in the collection
   * while the data file is append-only so it may grow larger
   * @returns {Promise<any>}
   */
  persistCachedDatabase() {
    return new Promise((resolve, reject) => {
      let toPersist = '';

      if (this.inMemoryOnly) {
        resolve(null);
        return;
      }

      // TODO: db.getAllData() => Promise
      this.db
        .getAllData()
        .then(data => {
          data.forEach(doc => {
            toPersist += this.afterSerialization(serialize(doc)) + '\n';
          });
          Object.keys(this.db.indexes).forEach(fieldName => {
            if (fieldName !== '_id') {
              // The special _id index is managed by datastore.js, the others need to be persisted
              toPersist +=
                this.afterSerialization(
                  serialize({
                    $$indexCreated: {
                      fieldName: fieldName,
                      unique: this.db.indexes[fieldName].unique,
                      sparse: this.db.indexes[fieldName].sparse
                    }
                  })
                ) + '\n';
            }
          });

          storage
            .crashSafeWriteFile(this.filename, toPersist)
            .then(() => {
              this.db.emit('compaction.done');
              resolve(null);
            })
            .catch(err => reject(err));
        })
        .catch(err => reject(err));
    });
  }

  /**
   * Queue a rewrite of the datafile
   */
  compactDatafile() {
    this.db.executor.push({ this: this, fn: this.persistCachedDatabase, arguments: [] });
  }

  /**
   * Set automatic compaction every interval ms
   * @param {Number} interval in milliseconds, with an enforced minimum of 5 seconds
   */
  setAutocompactionInterval(interval = 0) {
    const minInterval = 5000;
    const realInterval = Math.max(interval, minInterval);

    this.stopAutocompaction();

    this.autocompactionIntervalId = setInterval(() => {
      this.compactDatafile();
    }, realInterval);
  }

  /**
   * Stop autocompaction (do nothing if autocompaction was not running)
   */
  stopAutocompaction() {
    if (this.autocompactionIntervalId) {
      clearInterval(this.autocompactionIntervalId);
    }
  }

  /**
   * Persist new state for the given newDocs (can be insertion, update or removal)
   * Use an append-only format
   * @param {Array} newDocs Can be empty if no doc was updated/removed
   * @returns {Promise<any>}
   */
  persistNewState(newDocs) {
    let toPersist = '';
    return new Promise((resolve, reject) => {
      if (this.inMemoryOnly) {
        resolve(null);
        return;
      }

      newDocs.forEach(doc => {
        toPersist += this.afterSerialization(serialize(doc)) + '\n';
      });

      if (!toPersist.length) {
        resolve(null);
        return;
      }

      storage.appendFile(this.filename, toPersist, 'utf8', err => {
        if (err) {
          reject(err);
          return;
        }
        resolve();
      });
    });
  }

  /**
   * From a database's raw data, return the corresponding
   * machine understandable collection
   */
  // eslint-disable-next-line complexity
  treatRawData(rawData) {
    const data = rawData.split('\n');
    const dataLen = data.length;
    const dataById = {};
    const tdata = [];
    const indexes = {};
    let i = 0;
    let corruptItems = -1; // Last line of every data file is usually blank so not really corrupt

    for (; i < dataLen; i = i + 1) {
      let doc;

      try {
        doc = deserialize(this.beforeDeserialization(data[i]));
        // eslint-disable-next-line max-depth
        if (doc._id) {
          // eslint-disable-next-line max-depth
          if (doc.$$deleted === true) {
            delete dataById[doc._id];
          } else {
            dataById[doc._id] = doc;
          }
        } else if (doc.$$indexCreated && doc.$$indexCreated.fieldName !== undefined) {
          indexes[doc.$$indexCreated.fieldName] = doc.$$indexCreated;
        } else if (typeof doc.$$indexRemoved === 'string') {
          delete indexes[doc.$$indexRemoved];
        }
      } catch (e) {
        corruptItems += 1;
      }
    }

    // A bit lenient on corruption
    if (dataLen > 0 && corruptItems / dataLen > this.corruptAlertThreshold) {
      throw new Error(
        `More than ${Math.floor(
          100 * this.corruptAlertThreshold
        )}% of the data file is corrupt, the wrong beforeDeserialization hook may be used. Cautiously refusing to start NeDB to prevent dataloss`
      );
    }

    Object.keys(dataById).forEach(function(key) {
      tdata.push(dataById[key]);
    });

    return { data: tdata, indexes: indexes };
  }

  /**
   * Load the database
   * 1) Create all indexes
   * 2) Insert all data
   * 3) Compact the database
   * This means pulling data out of the data file or creating it if it doesn't exist
   * Also, all data is persisted right away, which has the effect of compacting the database file
   * This operation is very quick at startup for a big collection (60ms for ~10k docs)
   * @return {Promise<any>}
   */
  loadDatabase() {
    this.db.resetIndexes();

    // In-memory only datastore
    if (this.inMemoryOnly) {
      return Promise.resolve(null);
    }

    return Persistence.ensureDirectoryExists(dirname(this.filename))
      .then(() => Persistence.ensureDatafileIntegrity(this.filename))
      .then(() => {
        return new Promise((resolve, reject) => {
          storage.readFile(this.filename, 'utf8', (err, rawData) => {
            if (err) {
              reject(err);
              return;
            }

            let treatedData;
            try {
              treatedData = this.treatRawData(rawData);
            } catch (e) {
              reject(e);
              return;
            }

            // Recreate all indexes in the datafile
            Object.keys(treatedData.indexes).forEach(key => {
              this.db.indexes[key] = new Index(treatedData.indexes[key]);
            });

            // Fill cached database (i.e. all indexes) with data
            try {
              this.db.resetIndexes(treatedData.data);
            } catch (e) {
              this.db.resetIndexes(); // Rollback any index which didn't fail
              reject(e);
              return;
            }

            this.db.persistence.persistCachedDatabase().then(() => {
              resolve();
            });
          });
        });
      })
      .then(() => {
        const onEmpty = this.db.executor.onEmpty();
        this.db.executor.processBuffer();
        return onEmpty.then(() => null);
      });
  }
}
