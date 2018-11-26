# GCS Backed Repo

## Running

The GCS credentials must be updated

```js
const googleCredential = {
  'credentials': {
    'client_email': 'client_email',
    'private_key': 'private_key'
  },
  'apiKey': 'apiKey',
  'projectId': 'projectId'
}
```

About using Google Cloud Storage, [see more](https://cloud.google.com/appengine/docs/standard/nodejs/using-cloud-storage)

then, run the example:

```bash
npm i
node index.js
```