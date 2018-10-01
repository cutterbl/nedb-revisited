import { writeFile, open, close, fsync, exists, rename, unlink, appendFile, readFile } from 'fs';
import { dirname } from 'path';
import mkdirp from 'mkdirp';

export default class Storage {
  static exists = exists;
  static rename = rename;
  static writeFile = writeFile;
  static unlink = unlink;
  static appendFile = appendFile;
  static readFile = readFile;
  static mkdirp = mkdirp;

  static flushIfExists(filename) {
    return new Promise((resolve, reject) => {
      exists(filename, there => {
        if (there) {
          Storage.flushToStorage(filename)
            .then(() => {
              resolve();
            })
            .catch(err => {
              reject(err);
            });
        } else {
          resolve();
        }
      });
    });
  }

  static writeTempFile(filename, data) {
    return new Promise((resolve, reject) => {
      Storage.writeFile(filename, data, err => {
        if (err) {
          reject(err);
          return;
        }
        resolve();
      });
    });
  }

  static renameTempFile(fileName, newFileName) {
    return new Promise((resolve, reject) => {
      Storage.rename(fileName, newFileName, err => {
        if (err) {
          reject(err);
          return;
        }
        resolve();
      });
    });
  }

  static flushToStorage(options) {
    let filename;
    let flags;
    if (typeof options === 'string') {
      filename = options;
      flags = 'r+';
    } else {
      filename = options.filename;
      flags = options.isDir ? 'r' : 'r+';
    }

    // Windows can't fsync (FlushFileBuffers) directories. We can live with this as it cannot cause 100% dataloss
    // except in the very rare event of the first time database is loaded and a crash happens
    if (flags === 'r' && (process.platform === 'win32' || process.platform === 'win64')) {
      return Promise.resolve(null);
    }

    return new Promise((resolve, reject) => {
      open(filename, flags, (err, fd) => {
        if (err) {
          reject(err);
          return;
        }
        fsync(fd, errFS => {
          close(fd, errFC => {
            if (errFS || errFC) {
              const error = new Error('Failed to flush to storage');
              error.errorOnFsync = errFS;
              error.errorOnClose = errFC;
              reject(error);
              return;
            }
            resolve(null);
          });
        });
      });
    });
  }

  static crashSafeWriteFile(filename, data) {
    let tempFilename = filename + '~';

    return Storage.flushToStorage({ filename: dirname(filename), isDir: true })
      .then(() => Storage.flushIfExists(filename))
      .then(() => Storage.writeTempFile(tempFilename, data))
      .then(() => Storage.flushToStorage(tempFilename))
      .then(() => Storage.renameTempFile(tempFilename, filename))
      .then(() => Storage.flushToStorage({ filename: dirname(filename), isDir: true }));
  }

  static ensureFileDoesntExist(file) {
    return new Promise((resolve, reject) => {
      exists(file, fileExists => {
        if (!fileExists) {
          resolve(null);
          return;
        }

        unlink(file, err => {
          if (err) {
            reject(err);
            return;
          }
          resolve();
        });
      });
    });
  }

  static ensureDatafileIntegrity(filename) {
    const tempFilename = filename + '~';

    return new Promise((resolve, reject) => {
      exists(filename, filenameExists => {
        // Write was successful
        if (filenameExists) {
          resolve(null);
          return;
        }

        exists(tempFilename, oldFilenameExists => {
          // New database
          if (!oldFilenameExists) {
            writeFile(filename, '', 'utf8', err => {
              if (err) {
                // Write failed, use old version
                rename(tempFilename, filename, function(errRN) {
                  if (errRN) {
                    reject(errRN);
                  }
                  reject(err);
                });
                return;
              }
              resolve();
            });
          }
        });
      });
    });
  }
}
