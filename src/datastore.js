import { EventEmitter } from 'events';
import { isArray } from 'util';
import { date, string, number, bool, defined } from 'is';
import intersection from 'lodash.intersection';
import pluck from 'lodash.pluck';
import { uid } from './customUtils';
import { match, deepCopy, checkObject, modify } from './model';
import Persistence from './persistence';
import Executor from './executor';
import Index from './indexes';
import Cursor from './cursor';

const passthrough = s => s;

const remove = function(query, options = {}) {
  // let callback = false;
  let numRemoved = 0;
  const removedDocs = [];

  /*if (typeof options === 'function') {
    callback = options;
    options = {};
  }*/

  const multi = defined(options.multi) ? options.multi : false;

  return this.getCandidates(query, true).then(candidates => {
    try {
      candidates.forEach(d => {
        if (match(d, query) && (multi || numRemoved === 0)) {
          numRemoved += 1;
          removedDocs.push({ $$deleted: true, _id: d._id });
          this.removeFromIndexes(d);
        }
      });
    } catch (err) {
      return Promise.reject(err);
    }

    return this.persistence.persistNewState(removedDocs).then(() => numRemoved);
  });
};

/**
 * Prepare a document (or array of documents) to be inserted in a database
 * Meaning adds _id and timestamps if necessary on a copy of newDoc to avoid any side effect on user input
 * @api private
 */
const prepareDocumentForInsertion = function(newDoc) {
  let preparedDoc;
  if (isArray(newDoc)) {
    preparedDoc = [];
    newDoc.forEach(doc => {
      preparedDoc.push(prepareDocumentForInsertion.call(this, doc));
    });
  } else {
    preparedDoc = deepCopy(newDoc);
    if (preparedDoc._id === undefined) {
      preparedDoc._id = this.createNewId();
    }
    const now = new Date();
    if (this.timestampData && preparedDoc.createdAt === undefined) {
      preparedDoc.createdAt = now;
    }
    if (this.timestampData && preparedDoc.updatedAt === undefined) {
      preparedDoc.updatedAt = now;
    }
    checkObject(preparedDoc);
  }

  return preparedDoc;
};

/**
 * If one insertion fails (e.g. because of a unique constraint), roll back all previous
 * inserts and throws the error
 * @api private
 */
const insertMultipleDocsInCache = function(preparedDocs) {
  let failingI;
  let error;
  for (let i = 0; i < preparedDocs.length; i = i + 1) {
    try {
      this.addToIndexes(preparedDocs[i]);
    } catch (e) {
      error = e;
      failingI = i;
      break;
    }
  }

  if (error) {
    for (let i = 0; i < failingI; i = i + 1) {
      this.removeFromIndexes(preparedDocs[i]);
    }

    throw error;
  }
};

/**
 * If newDoc is an array of documents, this will insert all documents in the cache
 * @api private
 */
const insertInCache = function(preparedDoc) {
  if (isArray(preparedDoc)) {
    insertMultipleDocsInCache.call(this, preparedDoc);
  } else {
    this.addToIndexes(preparedDoc);
  }
};

/**
 * Insert a new document
 * @param {Object} newDoc
 *
 * @api private Use Datastore.insert which has the same signature
 */
const insert = function(newDoc) {
  let preparedDoc;
  try {
    preparedDoc = prepareDocumentForInsertion.call(this, newDoc);
    insertInCache.call(this, preparedDoc);
  } catch (e) {
    return Promise.reject(e);
  }

  return this.persistence
    .persistNewState(isArray(preparedDoc) ? preparedDoc : [preparedDoc])
    .then(() => deepCopy(preparedDoc));
};

const update = function(query, updateQuery, options) {
  const multi = options.multi !== undefined ? options.multi : false;
  const upsert = options.upsert !== undefined ? options.upsert : false;
  let numReplaced = 0;

  return new Promise((resolve, reject) => {
    // If upsert option is set, check whether we need to insert the doc
    if (!upsert) {
      resolve();
      return;
    }
    const cursor = new Cursor(this, query).limit(1);
    // Need to use an internal function not tied to the executor to avoid deadlock
    cursor
      ._exec()
      .then(docs => {
        if (docs.length === 1) {
          return Promise.resolve();
        }
        let toBeInserted;

        try {
          checkObject(updateQuery);
          // updateQuery is a simple object with no modifier, use it as the document to insert
          toBeInserted = updateQuery;
        } catch (e) {
          // updateQuery contains modifiers, use the find query as the base,
          // strip it from all operators and update it according to updateQuery
          try {
            toBeInserted = modify(deepCopy(query, true), updateQuery);
          } catch (err) {
            return Promise.reject(err);
          }
        }
        return insert.call(this, toBeInserted).then(doc => {
          resolve(doc);
        });
      })
      .then(() => {
        resolve();
      })
      .catch(error => {
        reject(error);
      });
  })
    .then(() => this.getCandidates(query))
    .then(
      candidates =>
        new Promise((resolve, reject) => {
          // Preparing update (if an error is thrown here neither the datafile nor
          // the in-memory indexes are affected)
          const modifications = [];
          // eslint-disable-next-line max-depth
          try {
            // eslint-disable-next-line max-depth
            for (let i = 0; i < candidates.length; i = i + 1) {
              // eslint-disable-next-line max-depth
              if (match(candidates[i], query) && (multi || numReplaced === 0)) {
                numReplaced += 1;
                let createdAt;
                // eslint-disable-next-line max-depth
                if (this.timestampData) {
                  createdAt = candidates[i].createdAt;
                }
                const modifiedDoc = modify(candidates[i], updateQuery);
                // eslint-disable-next-line max-depth
                if (this.timestampData) {
                  modifiedDoc.createdAt = createdAt;
                  modifiedDoc.updatedAt = new Date();
                }
                modifications.push({ oldDoc: candidates[i], newDoc: modifiedDoc });
              }
            }
          } catch (err) {
            return reject(err);
          }

          // Change the docs in memory
          try {
            this.updateIndexes(modifications);
          } catch (err) {
            return reject(err);
          }

          // Update the datafile
          let updatedDocs = pluck(modifications, 'newDoc');
          this.persistence.persistNewState(updatedDocs).then(() => {
            if (!options.returnUpdatedDocs) {
              return resolve(numReplaced);
            } else {
              let updatedDocsDC = [];
              updatedDocs.forEach(doc => {
                updatedDocsDC.push(deepCopy(doc));
              });
              if (!multi) {
                updatedDocsDC = updatedDocsDC[0];
              }
              updatedDocsDC.numReplaced = numReplaced;
              resolve(updatedDocsDC);
            }
          });
        })
    );
};

export default class DataStore extends EventEmitter {
  filename = null;
  inMemoryOnly = false;
  autoload = false;
  timeStampData = false;
  nodeWebkitAppName = null;
  corruptAlertThreshold = 0.1;
  compareStrings = null;
  beforeSerialization = passthrough;
  afterSerialization = passthrough;

  indexes = {};
  ttlIndexes = {};

  /**
   *
   * @param options (String|Object)
   *  Sting - filename
   *  Object - @param {String} filename Optional, datastore will be in-memory only if not provided
   *           @param {Boolean} inMemoryOnly [defaults to false]
   *           @param {Boolean} autoload [defaults to false]
   *           @param {Boolean} timeStampData [defaults to false] Optional, if set to true, createdAt and updatedAt will be created and populated automatically {if not specified by user}
   *           @param {String} nodeWebkitAppName Optional, specify the name of your NW app if you want options.filename to be relative to the directory where
   *                                            Node Webkit stores application data such as cookies and local storage {the best place to store data in my opinion}
   *           @param {Number} corruptAlertThreshold Optional, threshold after which an alert is thrown if too much data is corrupt
   *           @param {Function} compareStrings Optional, string comparison function that overrides default for sorting
   *           @param {Function} beforeSerialization Optional, serialization hooks
   *           @param {Function} afterSerialization Optional, serialization hooks
   * @returns {Promise<this>} Constructor returns a promise, regardless of if autoload was set or not. Resolves to 'this' instance.
   *
   * Event Emitter - Events
   * * compaction.done - Fired whenever a compaction operation was finished
   */
  constructor(options) {
    super();
    if (typeof options === 'string') {
      this.filename = options;
    } else {
      Object.assign(this, options);
    }

    // Determine whether in memory or persistent
    this.filename =
      !this.filename || typeof this.filename !== 'string' || this.filename.length === 0
        ? null
        : this.filename;
    this.inMemoryOnly = this.filename !== null;

    this.persistence = new Persistence({
      db: this,
      nodeWebkitAppName: this.nodeWebkitAppName,
      beforeSerialization: this.beforeSerialization,
      afterSerialization: this.afterSerialization,
      corruptAlertThreshold: this.corruptAlertThreshold
    });

    // This new executor is ready if we don't use persistence
    // If we do, it will only be ready once loadDatabase is called
    this.executor = new Executor();
    if (this.inMemoryOnly) {
      this.executor.ready = true;
    }

    // Indexed by field name, dot notation can be used
    // _id is always indexed and since _ids are generated randomly the underlying
    // binary is always well-balanced
    this.indexes._id = new Index({ fieldName: '_id', unique: true });

    // *** Breaking Change: constructor returns a promise that resolves to 'this'
    // Queue a load of the database right away and call the onload handler
    // By default (no onload handler), if there is an error there, no operation will be possible so warn the user by throwing an exception
    if (this.autoload) {
      return this.loadDatabase().then(() => this);
    }
    return Promise.resolve(this);
  }

  /**
   * Load the database from the datafile, and trigger the execution of buffered commands if any
   * @return {Promise}
   */
  loadDatabase() {
    return this.executor.push(() => this.persistence.loadDatabase(), true);
  }

  /**
   * Get an array of all the data in the database
   * @return {Promise<Array>}
   */
  getAllData() {
    return Promise.resolve(this.indexes._id.getAll());
  }

  /**
   * Reset all currently defined indexes
   * @param newData
   */
  resetIndexes(newData) {
    Object.keys(this.indexes).forEach(i => {
      this.indexes[i].reset(newData);
    });
  }

  /**
   * Ensure an index is kept for this field. Same parameters as lib/indexes
   * For now this function is synchronous, we need to test how much time it takes
   * We use an async API for consistency with the rest of the code
   * @param {String} options.fieldName
   * @param {Boolean} options.unique
   * @param {Boolean} options.sparse
   * @param {Number} options.expireAfterSeconds - Optional, if set this index becomes a TTL index (only works on Date fields, not arrays of Date)
   * @return {Promise<any>}
   */
  ensureIndex(options) {
    options = options || {};

    if (!options.fieldName) {
      const err = new Error('Cannot create an index without a fieldName');
      err.missingFieldName = true;
      return Promise.reject(err);
    }
    if (this.indexes[options.fieldName]) {
      return Promise.resolve(null);
    }

    this.indexes[options.fieldName] = new Index(options);
    if (options.expireAfterSeconds !== undefined) {
      this.ttlIndexes[options.fieldName] = options.expireAfterSeconds;
    } // With this implementation index creation is not necessary to ensure TTL but we stick with MongoDB's API here

    try {
      this.getAllData()
        .then(data => {
          this.indexes[options.fieldName].insert(data);
        })
        .catch(error => {
          throw error;
        });
      this.indexes[options.fieldName].insert(this.getAllData());
    } catch (e) {
      delete this.indexes[options.fieldName];
      return Promise.reject(e);
    }

    // We may want to force all options to be persisted including defaults, not just the ones passed the index creation function
    return this.persistence.persistNewState([{ $$indexCreated: options }]);
  }

  /**
   * Remove an index
   * @param {String} fieldName
   * @return {*|Promise<any>}
   */
  removeIndex(fieldName) {
    delete this.indexes[fieldName];

    return this.persistence.persistNewState([{ $$indexRemoved: fieldName }]);
  }

  /**
   * Add one or several document(s) to all indexes
   * @param {Object|Array} doc
   */
  addToIndexes(doc) {
    const keys = Object.keys(this.indexes);

    let failingIndex;
    let error;
    for (let i = 0; i < keys.length; i = i + 1) {
      try {
        this.indexes[keys[i]].insert(doc);
      } catch (e) {
        failingIndex = i;
        error = e;
        break;
      }
    }

    // If an error happened, we need to rollback the insert on all other indexes
    if (error) {
      for (let i = 0; i < failingIndex; i = i + 1) {
        this.indexes[keys[i]].remove(doc);
      }

      throw error;
    }
  }

  /**
   * Remove one or several document(s) from all indexes
   * @param {Object|Array} doc
   */
  removeFromIndexes(doc) {
    Object.keys(this.indexes).forEach(i => {
      this.indexes[i].remove(doc);
    });
  }

  /**
   * Update one or several documents in all indexes
   * To update multiple documents, oldDoc must be an array of { oldDoc, newDoc } pairs
   * If one update violates a constraint, all changes are rolled back
   * @param oldDoc
   * @param newDoc
   */
  updateIndexes(oldDoc, newDoc) {
    const keys = Object.keys(this.indexes);

    let failingIndex;
    let error;
    for (let i = 0; i < keys.length; i = i + 1) {
      try {
        this.indexes[keys[i]].update(oldDoc, newDoc);
      } catch (e) {
        failingIndex = i;
        error = e;
        break;
      }
    }

    // If an error happened, we need to rollback the update on all other indexes
    if (error) {
      for (let i = 0; i < failingIndex; i = i + 1) {
        this.indexes[keys[i]].revertUpdate(oldDoc, newDoc);
      }

      throw error;
    }
  }

  removeAllExpiredDocs(docs, dontExpireStaleDocs) {
    return new Promise(resolve => {
      if (dontExpireStaleDocs) {
        resolve(docs);
        return;
      }

      const expiredDocIds = [];
      const validDocs = [];
      const ttlIndexesFieldNames = Object.keys(this.ttlIndexes);

      docs.forEach(doc => {
        let valid = true;
        ttlIndexesFieldNames.forEach(i => {
          if (
            defined(
              doc[i] && date(doc[i]) && Date.now() > doc[i].getTime() + this.ttlIndexes[i] * 1000
            )
          ) {
            valid = false;
          }
        });
        if (valid) {
          validDocs.push(doc);
        } else {
          expiredDocIds.push(doc._id);
        }
      });

      const series = Promise.resolve();
      if (expiredDocIds.length) {
        expiredDocIds.forEach(_id => {
          series.then(() => remove.call(this, { _id: _id }, {}));
        });
      }
      series.then(() => {
        resolve(validDocs);
      });
    });
  }

  getCandidatesFromIndexes(query, indexNames) {
    return new Promise((resolve, reject) => {
      let usableQueryKeys = [];
      Object.keys(query).forEach(key => {
        if (
          string(query[key]) ||
          number(query[key]) ||
          bool(query[key]) ||
          date(query[key]) ||
          query[key] === null
        ) {
          usableQueryKeys.push(key);
        }
      });
      usableQueryKeys = intersection(usableQueryKeys, indexNames);
      if (usableQueryKeys.length) {
        resolve(this.indexes[usableQueryKeys[0]].getMatching(query[usableQueryKeys[0]]));
        return;
      }

      // For a $in match
      usableQueryKeys = [];
      Object.keys(query).forEach(key => {
        if (query[key] && query[key].hasOwnProperty('$in')) {
          usableQueryKeys.push(key);
        }
      });
      usableQueryKeys = intersection(usableQueryKeys, indexNames);
      if (usableQueryKeys.length) {
        resolve(this.indexes[usableQueryKeys[0]].getMatching(query[usableQueryKeys[0]].$in));
        return;
      }

      // For a comparison match
      usableQueryKeys = [];
      Object.keys(query).forEach(key => {
        if (
          query[key] &&
          (query[key].hasOwnProperty('$lt') ||
            query[key].hasOwnProperty('$lte') ||
            query[key].hasOwnProperty('$gt') ||
            query[key].hasOwnProperty('$gte'))
        ) {
          usableQueryKeys.push(key);
        }
      });
      usableQueryKeys = intersection(usableQueryKeys, indexNames);
      if (usableQueryKeys.length) {
        resolve(this.indexes[usableQueryKeys[0]].getBetweenBounds(query[usableQueryKeys[0]]));
        return;
      }
      this.getAllData()
        .then(data => {
          resolve(data);
        })
        .catch(err => {
          reject(err);
        });
    });
  }

  getCandidates(query, dontExpireStaleDocs) {
    const indexNames = Object.keys(this.indexes);
    let callback = false;

    if (typeof dontExpireStaleDocs === 'function') {
      callback = dontExpireStaleDocs;
      dontExpireStaleDocs = false;
    }

    const run = this.getCandidatesFromIndexes(query, indexNames)
      .then(docs => this.removeAllExpiredDocs(docs, dontExpireStaleDocs))
      .catch(err => Promise.reject(err));

    return !callback ? run : run.then(docs => callback(docs));
  }

  /**
   * Create a new _id that's not already in use
   */
  createNewId() {
    let tentativeId = uid(16);
    // Try as many times as needed to get an unused _id. As explained in customUtils, the probability of this ever happening is extremely small, so this is O(1)
    if (this.indexes._id.getMatching(tentativeId).length) {
      tentativeId = this.createNewId();
    }
    return tentativeId;
  }

  insert(newDoc) {
    return this.executor.push(() => insert.call(this, newDoc), true);
  }

  /**
   * Find all documents matching the query
   * If no callback is passed, we return the cursor so that user can limit, skip and finally exec
   * @param {Object} query MongoDB-style query
   * @param {Object} projection MongoDB-style projection
   * @param {Boolean} immediate Optional, defaults to false
   * @return {Promise|Cursor} if immediate, returns the Cursor.exec Promise, else returns the cursor instance
   */
  find(query, projection = {}, immediate = false) {
    /*let callback = false;
    if (typeof projection === 'function') {
      callback = projection;
      projection = {};
    }*/

    const cursor = new Cursor(this, query, docs => {
      const res = [];
      docs.forEach(doc => {
        res.push(deepCopy(doc));
      });
      res.totalCount = docs.totalCount;
      return Promise.resolve(res);
    });

    cursor.projection(projection);

    return immediate
      ? cursor.exec().catch(err => {
          console.log(err);
        })
      : cursor;
  }

  /**
   * Find one document matching the query
   * @param {Object} query MongoDB-style query
   * @param {Object} projection MongoDB-style projection
   */
  findOne(query, projection = {}, immediate = false) {
    /*let callback = false;
    if (typeof projection === 'function') {
      callback = projection;
      projection = {};
    }*/

    const cursor = new Cursor(this, query, docs => {
      if (docs.length === 1) {
        return Promise.resolve(deepCopy(docs[0]));
      } else {
        return Promise.resolve(null);
      }
    });

    cursor.projection(projection).limit(1);

    return immediate ? cursor.exec() : cursor;
  }

  update(query, updateQuery, options) {
    return this.executor.push(() => update.call(this, query, updateQuery, options), true);
  }

  remove(query, options) {
    return this.executor.push(() => remove.call(this, query, options), true);
  }
}
