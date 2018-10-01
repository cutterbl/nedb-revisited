import { AVLTree as BinarySearchTree } from 'binary-search-tree';
import { isArray } from 'util';
import uniq from 'lodash.uniq';
import { compareThings, getDotValue } from './model';
/**
 * Two indexed pointers are equal iif they point to the same place
 */
const checkValueEquality = function(a, b) {
  return a === b;
};

/**
 * Type-aware projection
 */
const projectForUnique = function(elt) {
  if (elt === null) {
    return '$null';
  }
  if (typeof elt === 'string') {
    return '$string' + elt;
  }
  if (typeof elt === 'boolean') {
    return '$boolean' + elt;
  }
  if (typeof elt === 'number') {
    return '$number' + elt;
  }
  if (isArray(elt)) {
    return '$date' + elt.getTime();
  }

  return elt; // Arrays and objects, will check for pointer equality
};

export default class Index {
  fieldName = '';
  unique = false;
  sparse = false;

  constructor(options) {
    Object.assign(this, options);

    this.treeOptions = {
      unique: this.unique,
      compareKeys: compareThings,
      checkValueEquality: checkValueEquality
    };

    this.reset();
  }

  /**
   * Reset an index
   * @param {Document or Array of documents} newData Optional, data to initialize the index with
   *                                                 If an error is thrown during insertion, the index is not modified
   */
  reset(newData) {
    this.tree = new BinarySearchTree(this.treeOptions);

    if (newData) {
      this.insert(newData);
    }
  }

  /**
   * Insert a new document in the index
   * If an array is passed, we insert all its elements (if one insertion fails the index is not modified)
   * @param doc {String|Array}
   */
  insert(doc) {
    if (isArray(doc)) {
      this.insertMultipleDocs(doc);
      return;
    }

    const key = getDotValue(doc, this.fieldName);

    // We don't index documents that don't contain the field if the index is sparse
    if (key === undefined && this.sparse) {
      return;
    }

    if (!isArray(key)) {
      this.tree.insert(key, doc);
    } else {
      // If an insert fails due to a unique constraint, roll back all inserts before it
      let keys = uniq(key, projectForUnique);

      let failingI;
      let error;

      for (let i = 0; i < keys.length; i = i + 1) {
        // eslint-disable-next-line max-depth
        try {
          this.tree.insert(keys[i], doc);
        } catch (e) {
          error = e;
          failingI = i;
          break;
        }
      }

      if (error) {
        // eslint-disable-next-line max-depth
        for (let i = 0; i < failingI; i = i + 1) {
          this.tree.delete(keys[i], doc);
        }

        throw error;
      }
    }
  }

  /**
   * Insert an array of documents in the index
   * If a constraint is violated, the changes should be rolled back and an error thrown
   *
   * @API private
   * TODO: move it out of the class
   */
  insertMultipleDocs(docs) {
    let error;
    let failingI;

    for (let i = 0; i < docs.length; i = i + 1) {
      try {
        this.insert(docs[i]);
      } catch (e) {
        error = e;
        failingI = i;
        break;
      }
    }

    if (error) {
      for (let i = 0; i < failingI; i = i + 1) {
        this.remove(docs[i]);
      }

      throw error;
    }
  }

  /**
   * Remove a document from the index
   * If an array is passed, we remove all its elements
   * The remove operation is safe with regards to the 'unique' constraint
   * @param doc
   */
  remove(doc) {
    if (isArray(doc)) {
      doc.forEach(d => {
        this.remove(d);
      });
      return;
    }

    const key = getDotValue(doc, this.fieldName);

    if (key === undefined && this.sparse) {
      return;
    }

    if (!isArray(key)) {
      this.tree.delete(key, doc);
    } else {
      uniq(key, projectForUnique).forEach(_key => {
        this.tree.delete(_key, doc);
      });
    }
  }

  /**
   * Update a document in the index
   * If a constraint is violated, changes are rolled back and an error thrown
   * Naive implementation, still in O(log(n))
   * @param oldDoc
   * @param newDoc
   */
  update(oldDoc, newDoc) {
    if (isArray(oldDoc)) {
      this.updateMultipleDocs(oldDoc);
      return;
    }

    this.remove(oldDoc);

    try {
      this.insert(newDoc);
    } catch (e) {
      this.insert(oldDoc);
      throw e;
    }
  }

  /**
   * Update multiple documents in the index
   * If a constraint is violated, the changes need to be rolled back
   * and an error thrown
   * @param {Array of oldDoc, newDoc pairs} pairs
   *
   * @API private
   * TODO: move it out of class
   */
  updateMultipleDocs(pairs) {
    for (let i = 0; i < pairs.length; i = i + 1) {
      this.remove(pairs[i].oldDoc);
    }

    let failingI;
    let error;
    for (let i = 0; i < pairs.length; i = i + 1) {
      try {
        this.insert(pairs[i].newDoc);
      } catch (e) {
        error = e;
        failingI = i;
        break;
      }
    }

    // If an error was raised, roll back changes in the inverse order
    if (error) {
      for (let i = 0; i < failingI; i = i + 1) {
        this.remove(pairs[i].newDoc);
      }

      for (let i = 0; i < pairs.length; i = i + 1) {
        this.insert(pairs[i].oldDoc);
      }

      throw error;
    }
  }

  /**
   * Revert an update
   * @param oldDoc
   * @param newDoc
   */
  revertUpdate(oldDoc, newDoc) {
    const revert = [];

    if (!isArray(oldDoc)) {
      this.update(newDoc, oldDoc);
    } else {
      oldDoc.forEach(pair => {
        revert.push({ oldDoc: pair.newDoc, newDoc: pair.oldDoc });
      });
      this.update(revert);
    }
  }

  /**
   * Get all documents in index whose key match value (if it is a Thing) or one of the elements of value (if it is an array of Things)
   * @param {Thing} value Value to match the key against
   * @return {Array of documents}
   */
  getMatching(value) {
    if (!isArray(value)) {
      return this.tree.search(value);
    } else {
      const _res = {};
      const res = [];

      value.forEach(v => {
        this.getMatching(v).forEach(doc => {
          _res[doc._id] = doc;
        });
      });

      Object.keys(_res).forEach(_id => {
        res.push(_res[_id]);
      });

      return res;
    }
  }

  /**
   * Get all documents in index whose key is between bounds are they are defined by query
   * Documents are sorted by key
   * @param {Query} query
   * @return {Array of documents}
   */
  getBetweenBounds(query) {
    return this.tree.betweenBounds(query);
  }

  /**
   * Get all elements in the index
   * @return {Array of documents}
   */
  getAll() {
    const res = [];

    this.tree.executeOnEveryNode(node => {
      for (let i = 0; i < node.data.length; i = i + 1) {
        res.push(node.data[i]);
      }
    });

    return res;
  }
}
