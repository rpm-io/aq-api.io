const rpmdb = require('./src/rpmdb')
const rpmClient = require('rpm.io-client')

let database = [{}]

rpmdb.config({
    port: 3701,
    async insert(data){
        database.push(data)
        return database.length - 1
    },
    async delete(id){
        const data = database[id]
        database = database.filter((value, index) => {
            return index != id
        })
        return data
    },
    async update(id, newData){
        const oldData = database[id]
        Object.assign(database[id], newData)
        return oldData
    },
    async findById(id){
        return database[id]
    }
})

rpmdb.start()

rpmClient.require_remote('http://localhost:3701')
.then(async remote => {
    const tran = await remote.startTransacton.__call__()
    console.log(tran)
    console.log(database)
    await remote.delete.__call__(tran, 0)
    console.log(database)
    const id = await remote.insert.__call__(tran, { foo: 'var'})
    console.log(id, database)
    await remote.update.__call__(tran, id, { foo2: 'var'})
    console.log(database)
    await remote.rollback.__call__(tran)
    console.log(database)
})