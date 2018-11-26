'use strict'

const IPFS = require('ipfs')
const Repo = require('ipfs-repo')
const { Storage } = require('@google-cloud/storage')
const GCSStore = require('datastore-gcs')
const RepoLock = require('./repro-lock')

// Initialize the gcs instance
const googleCredential = {
  'credentials': {
    'client_email': 'client_email',
    'private_key': 'private_key'
  },
  'apiKey': 'apiKey',
  'projectId': 'projectId'
}

const bucketName = 'ipfs-gcs'
const storage = new Storage(googleCredential)
const gcs = storage.bucket(bucketName)

// Create our custom lock
const gcsStore = new GCSStore('/', { gcs })
const repoLock = new RepoLock(gcsStore, 'ipfs-lock')

// Create the IPFS Repo, full backed by GCS
const repo = new Repo('ipfs-data', {
  storageBackends: {
    root: GCSStore,
    blocks: GCSStore,
    keys: GCSStore,
    datastore: GCSStore
  },
  storageBackendOptions: {
    root: { gcs },
    blocks: { gcs },
    keys: { gcs },
    datastore: { gcs }
  },
  lock: repoLock
})

// Create a new IPFS node with our GCS backed Repo
let node = new IPFS({
  repo,
  config: {
    Discovery: { MDNS: { Enabled: false }, webRTCStar: { Enabled: false } },
    Bootstrap: []
  }
})

console.log('Start the node')

// Test out the repo by sending and fetching some data
node.on('ready', () => {
  console.log('Ready')
  node.version()
    .then((version) => {
      console.log('Version:', version.version)
    })
    // Once we have the version, let's add a file to IPFS
    .then(() => {
      return node.files.add({
        path: 'data.txt',
        content: Buffer.from(`${new Date().toString()}`)
      })
    })
    // Log out the added files metadata and cat the file from IPFS
    .then((filesAdded) => {
      console.log('\nAdded file:', filesAdded[0].path, filesAdded[0].hash)
      return node.files.cat(filesAdded[0].hash)
    })
    // Print out the files contents to console
    .then((data) => {
      console.log(`\nFetched file content containing ${data.byteLength} bytes, content: ${data.toString()}`)
    })
    // Log out the error, if there is one
    .catch((err) => {
      console.log('File Processing Error:', err)
    })
    // After everything is done, shut the node down
    // We don't need to worry about catching errors here
    .then(() => {
      console.log('\n\nStopping the node')
      return node.stop()
    })
})
