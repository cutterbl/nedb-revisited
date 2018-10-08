/**
 * Manage access to data, be it to find, update or remove it
 */
import omit from 'lodash.omit';
import sortOn from 'sort-on';
import { getDotValue, modify, match } from './model';

/**
 * Apply the projection
 */
const project = function(candidates) {
  const res = [];

  if (this._projection === undefined || Object.keys(this._projection).length === 0) {
    return candidates;
  }

  const keepId = this._projection._id === 0 ? false : true;
  this._projection = omit(this._projection, '_id');

  // Check for consistency
  const keys = Object.keys(this._projection);
  let action;
  keys.forEach(key => {
    if (action !== undefined && this._projection[key] !== action) {
      throw new Error("Can't both keep and omit fields except for _id");
    }
    action = this._projection[key];
  });

  // Do the actual projection
  candidates.forEach(candidate => {
    let toPush;
    if (action === 1) {
      // pick-type projection
      toPush = { $set: {} };
      keys.forEach(key => {
        toPush.$set[key] = getDotValue(candidate, key);
        if (toPush.$set[key] === undefined) {
          delete toPush.$set[key];
        }
      });
      toPush = modify({}, toPush);
    } else {
      // omit-type projection
      toPush = { $unset: {} };
      keys.forEach(key => {
        toPush.$unset[key] = true;
      });
      toPush = modify(candidate, toPush);
    }
    if (keepId) {
      toPush._id = candidate._id;
    } else {
      delete toPush._id;
    }
    res.push(toPush);
  });

  return res;
};

/**
 * Create a new cursor for this collection
 * @param {Datastore} db - The datastore this cursor is bound to
 * @param {Query} query - The query this cursor will operate on
 * @param {Function} execFn - Handler to be executed after cursor has found the results and before the callback passed to find/findOne/update/remove
 */
export default class Cursor {
  constructor(db, query, execFn = s => s) {
    this.db = db;
    this.query = query;
    this.execFn = execFn;
  }

  /**
   * Set a limit to the number of results
   */
  limit(limit) {
    this._limit = limit;
    return this;
  }

  /**
   * Skip a the number of results
   */
  skip(skip) {
    this._skip = skip;
    return this;
  }

  /**
   * Sort results of the query
   * @param {SortQuery} sortQuery - SortQuery is { field: order }, field can use the dot-notation, order is 1 for ascending and -1 for descending
   */
  sort(sortQuery) {
    this._sort = sortQuery;
    return this;
  }

  /**
   * Add the use of a projection
   * @param {Object} projection - MongoDB-style projection. {} means take all fields. Then it's { key1: 1, key2: 1 } to take only key1 and key2
   *                              { key1: 0, key2: 0 } to omit only key1 and key2. Except _id, you can't mix takes and omits
   */
  projection(projection) {
    this._projection = projection;
    return this;
  }

  /**
   * Get all matching elements
   * Will return pointers to matched elements (shallow copies), returning full copies is the role of find or findOne
   * This is an internal function, use exec which uses the executor
   */
  _exec() {
    let res = [];
    let added = 0;
    let skipped = 0;

    // eslint-disable-next-line complexity
    return this.db.getCandidates(this.query).then(candidates => {
      try {
        for (let i = 0; i < candidates.length; i = i + 1) {
          // eslint-disable-next-line max-depth
          if (match(candidates[i], this.query)) {
            // If a sort is defined, wait for the results to be sorted before applying limit and skip
            // eslint-disable-next-line max-depth
            if (!this._sort) {
              // eslint-disable-next-line max-depth
              if (this._skip && this._skip > skipped) {
                skipped += 1;
              } else {
                res.push(candidates[i]);
                added += 1;
                // eslint-disable-next-line max-depth
                if (this._limit && this._limit <= added) {
                  break;
                }
              }
            } else {
              res.push(candidates[i]);
            }
          }
        }
      } catch (err) {
        return Promise.reject(err);
      }

      // Apply all sorts
      let totalCount = 0;
      if (this._sort) {
        const keys = Object.keys(this._sort);

        // Sorting
        const criteria = [];
        keys.forEach(key => {
          criteria.push(`${this._sort[key] < 0 ? '-' : ''}${key}`);
        });

        res = sortOn(res, criteria.length > 1 ? criteria : criteria[0]);

        totalCount = res.length; // get totalCount prior to slicing it up
        // Applying limit and skip
        const limit = this._limit || res.length;
        const skip = this._skip || 0;

        res = res.slice(skip, skip + limit);
      } else {
        // no sort
        totalCount = res.length;
      }

      // Apply projection
      try {
        res = project.call(this, res);
      } catch (e) {
        return Promise.reject(e);
      }
      res.totalCount = totalCount;

      return Promise.resolve(res);
    });
  }

  exec() {
    return this.db.executor.push(() => this._exec(), true).then(data => this.execFn(data));
  }
}
