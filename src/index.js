'use strict'
const assert = require('assert')
const path = require('upath')

const IDatastore = require('interface-datastore')
const Key = IDatastore.Key
const Errors = IDatastore.Errors

const Deferred = require('pull-defer')
const pull = require('pull-stream')

class GCSDataStore {
  constructor(path, opts = {}) {
    this.path = path
    this.opts = opts

    const {
      createIfMissing = false,
      gcs: { name } = {}
    } = opts

    assert(typeof name === 'string', 'An GCS instance with a predefined Bucket must be supplied. See the datastore-gcs README for examples.')
    assert(typeof createIfMissing === 'boolean', `createIfMissing must be a boolean but was (${typeof createIfMissing}) ${createIfMissing}`)

    this.bucket = name
    this.createIfMissing = createIfMissing
  }

  _getFullKey(key) {
    return path.join('.', this.path, key.toString())
  }

  /**
   * Check for the existence of the given key.
   *
   * @param {Key} key
   * @param {function(Error, bool)} callback
   * @returns {void}
   */
  has(key /* : Key */, callback /* : Callback<bool> */) /* : void */ {
    const fullKey = this._getFullKey(key)
    this.opts.gcs.file(fullKey).exists().then(([exists]) => {
      callback(null, exists)
    })
      .catch(err => {
        callback(err, false)
      })
  }

  /**
   * Store the given value under the key.
   *
   * @param {Key} key
   * @param {Buffer} val
   * @param {function(Error)} callback
   * @returns {void}
   */
  put(key /* : Key */, val /* : Buffer */, callback /* : Callback<void> */) /* : void */ {
    const fullKey = this._getFullKey(key)
    const writeStream = this.opts.gcs
      .file(fullKey)
      .createWriteStream()

    writeStream.on('error', (err) => {
      if (err && err.code === 404) {
        return this.opts.gcs.create((err, bucket) => {
          if (err) {
            return callback(err)
          }
          setImmediate(() => this.put(key, val, callback))
        })
      }

      return callback(Errors.dbWriteFailedError(err))
    })

    writeStream.on('finish', () => {
      return callback()
    })

    writeStream.end(val)
  }

  /**
  * Read from GCS.
  *
  * @param {Key} key
  * @param {function(Error, Buffer)} callback
  * @returns {void}
  */
  get(key /* : Key */, callback /* : Callback<Buffer> */) /* : void */ {
    const fullKey = this._getFullKey(key)

    this.opts.gcs.file(fullKey)
      .download()
      .then(contents => callback(null, contents))
      .catch(err => {
        if (err.code === 404) {
          callback(Errors.notFoundError(err))
        } else {
          callback(err)
        }
      })
  }

  /**
   * Delete the record under the given key.
   *
   * @param {Key} key
   * @param {function(Error)} callback
   * @returns {void}
   */
  delete(key /* : Key */, callback /* : Callback<void> */) /* : void */ {
    const fullKey = this._getFullKey(key)
    this.opts.gcs.file(fullKey)
      .delete()
      .then(callback)
      .catch(err => {
        if (err.code === 404) {
          callback(Errors.notFoundError(err))
        } else {
          callback(Errors.dbDeleteFailedError(err))
        }
      })
  }

  /**
   * Recursively fetches all keys from GCS
   * @param {Object} params
   * @param {Array<Key>} keys
   * @param {function} callback
   * @returns {void}
   */
  _listKeys(params /* : { prefix: string, pageToken: ?string } */, keys /* : Array<Key> */, callback /* : Callback<void> */) {
    if (typeof callback === 'undefined') {
      callback = keys
      keys = []
    }

    this.opts.gcs.getFiles(params, (err, files, nextQuery) => {
      if (err) {
        return callback(new Error(err.code))
      }

      const keys = files.map(file => new Key(file.name.slice(this.path.length), false))
      callback(null, keys)
    })
  }

  /**
   * Returns an iterator for fetching objects from gcs by their key
   * @param {Array<Key>} keys
   * @param {Boolean} keysOnly Whether or not only keys should be returned
   * @returns {Iterator}
   */
  _getGCSIterator(keys /* : Array<Key> */, keysOnly /* : boolean */) {
    let count = 0

    return {
      next: (callback/* : Callback<Error, Key, Buffer> */) => {
        if (count >= keys.length) {
          return callback(null, null, null)
        }

        let currentKey = keys[count++]

        if (keysOnly) {
          return callback(null, currentKey, null)
        }

        // Fetch the object Buffer from s3
        this.get(currentKey, (err, data) => {
          callback(err, currentKey, data)
        })
      }
    }
  }

  /**
   * Query the store.
   *
   * @param {Object} q
   * @returns {PullStream}
   */
  query(q /* : Query<Buffer> */) /* : QueryResult<Buffer> */ {
    const prefix = path.join(this.path, q.prefix || '')

    let deferred = Deferred.source()
    let iterator

    const params = {
      prefix
    }

    // this gets called recursively, the internals need to iterate
    const rawStream = (end, callback) => {
      if (end) {
        return callback(end)
      }

      iterator.next((err, key, value) => {
        if (err) {
          return callback(err)
        }

        // If the iterator is done, declare the stream done
        if (err === null && key === null && value === null) {
          return callback(true) // eslint-disable-line standard/no-callback-literal
        }

        const res /* : Object */ = {
          key: key
        }

        if (value) {
          res.value = value
        }

        callback(null, res)
      })
    }

    this._listKeys(params, (err, keys) => {
      if (err) {
        return deferred.abort(err)
      }

      iterator = this._getGCSIterator(keys, q.keysOnly || false)

      deferred.resolve(rawStream)
    })

    // Use a deferred pull stream source, as async operations need to occur before the
    // pull stream begins
    let tasks = [deferred]

    if (q.filters != null) {
      tasks = tasks.concat(q.filters.map(f => asyncFilter(f)))
    }

    if (q.orders != null) {
      tasks = tasks.concat(q.orders.map(o => asyncSort(o)))
    }

    if (q.offset != null) {
      let i = 0
      tasks.push(pull.filter(() => i++ >= q.offset))
    }

    if (q.limit != null) {
      tasks.push(pull.take(q.limit))
    }

    return pull.apply(null, tasks)
  }

  /**
   * Create a new batch object.
   *
   * @returns {Batch}
   */
  batch() /* : Batch */ {
    let puts = []
    let deletes = []

    return {
      put(key /* : Key */, value /* : Buffer */) /* : void */ {
        puts.push({ key: key, value: value })
      },
      delete(key /* : Key */) /* : void */ {
        deletes.push(key)
      },
      commit: (callback /* : (err: ?Error) => void */) => {
        waterfall([
          (cb) => each(puts, (p, _cb) => {
            this.put(p.key, p.value, _cb)
          }, cb),
          (cb) => each(deletes, (key, _cb) => {
            this.delete(key, _cb)
          }, cb)
        ], (err) => callback(err))
      }
    }
  }

  /**
   * This will check the s3 bucket to ensure access and existence
   *
   * @param {function(Error)} callback
   * @returns {void}
   */
  open(callback /* : Callback<void> */) /* : void */ {
    this.opts.gcs.getMetadata(
      (err) => {
        if (err && err.code === 404) {
          return this.put(new Key('/', false), Buffer.from(''), callback)
        }

        if (err) {
          return callback(Errors.dbOpenFailedError(err))
        }

        callback()
      })
  }

  /**
   * Close the store.
   *
   * @param {function(Error)} callback
   * @returns {void}
   */
  close(callback /* : (err: ?Error) => void */) /* : void */ {
    setImmediate(callback)
  }
}

module.exports = GCSDataStore