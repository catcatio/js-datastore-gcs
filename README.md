# js-datastore-gcs

Datastore implementation backed by Google Cloud Storage

## Usage

```js
const bucketName = '.ipfs'
const storage = new Storage()
const gcs = storage.bucket(bucketName)
const store = new GCSStore('/', {
  gcs,
  createIfMissing: false
})
```

### Examples

You can see examples of GCS backed ipfs in the [examples folder](examples/)
