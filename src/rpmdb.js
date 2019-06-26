const rpm = require('rpm.io')
const rpmClient = require('rpm.io-client')
const uuidV1 = require('uuid/v1')

let transactions = []
const databases = []

const primitives = {
    protocol: 'http',
    port: 7305,
    host: 'localhost',

    shareport: 7306,

    insert(data){ throw "not configured yet" },

    delete(id){ throw "not configured yet" },

    update(id, data){ throw "not configured yet" },

    findById(id){ throw "not configured yet" },
}

findTransaction = (uuid) => {
    const filtred_transactions = transactions.filter(transaction => transaction.uuid == uuid)
    if (filtred_transactions.length){
        return filtred_transactions[0]
    }
    throw "uuid not valid!"
}

removeTransaction = (uuid) => {
    transactions = transactions.filter(transaction => transaction.uuid != uuid)
}

findDb = ({ protocol, host, shareport }) => {
    const uri = `${protocol}://${host}:${shareport}`;
    if (!databases[uri]){
        databases[uri] = rpmClient.require_remote(uri)
    }
    return databases[uri]
}


const config = (new_config) => {
    for (let i in primitives){
        if (new_config[i]){
            primitives[i] = new_config[i]
        }
    }
}


const transaction_encode = (transaction) => Buffer.from(JSON.stringify(transaction)).toString('base64')
const transaction_decode = (transaction) => JSON.parse(Buffer.from(transaction, 'base64').toString('ascii'))

const start = () => {
    
    rpm.exports({
        hello: () => 1,
        addRollback(uuid, name, rollback){
            const transaction = findTransaction(uuid)
            transaction.tasks.push({ name, rollback })
        }
    }, primitives.shareport)

    rpm.exports({
        startTransacton() {
            const uuid = uuidV1()
            transactions.push({
                uuid,
                tasks: []
            })
            return transaction_encode({ uuid, host: primitives.host, port: primitives.port, protocol: primitives.protocol, shareport: primitives.shareport })
        },

        insert(transaction_hash, data) {
            const { uuid, ...dbconf } = transaction_decode(transaction_hash)
            return findDb(dbconf).then(db => {
                return primitives.insert(data).then( id => {
                    return db.addRollback.__call__(uuid, 'insert', () => {
                        return primitives.delete(id)
                    }).then(() => id)
                })
            })
            
        },

        delete(transaction_hash, id) {
            const { uuid, ...dbconf } = transaction_decode(transaction_hash)
            return findDb(dbconf).then(db => {
                return primitives.findById(id).then(data => {
                    return primitives.delete(id).then( () => {
                        return db.addRollback.__call__(uuid, 'delete', () => {
                            return primitives.insert(data)
                        }).then(() => data)
                    })
                })
            })
        },

        update(transaction_hash, id, newData) {
            const { uuid, ...dbconf } = transaction_decode(transaction_hash)
            return findDb(dbconf).then(db => {
                return primitives.findById(id).then(oldData => {
                    return primitives.update(id, newData).then( () => {
                        return db.addRollback.__call__(uuid, 'update', () => {
                            primitives.update(id, oldData)
                        }).then(() => oldData)
                    })
                })
            })
        },

        commit(transaction_hash) {
            const { uuid } = transaction_decode(transaction_hash)
            removeTransaction(uuid)
        },

        rollback(transaction_hash) {
            const { uuid } = transaction_decode(transaction_hash)
            const transaction = findTransaction(uuid)
            
            const rollbacks = transaction.tasks.reverse().map(task => task.rollback())
            return Promise.all(rollbacks).then((result) => {
                removeTransaction(uuid)
            })
        }
    }, primitives.port)
}

module.exports = {
    start, config
}