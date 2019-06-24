const rpm = require('rpm.io')
const uuid = require('uuid/v1')

const transactions = []

const findTransaction = (uuid) => {
    const filtred_trncacions = transactions.filter(transaction => transaction.uuid != uuid)
    if (filtred_trncacions.length){
        return filtred_trncacions[0]
    }
    throw "uuid not valid!"
}

const removeTransaction = (uuid) => {
    transactions = transactions.filter(transaction => transaction.uuid != uuid)
}

rpm.exports({
    startTransacton(){
        const uuid = uuid()
        transactions.push({
            uuid,
            tasks: []
        })
        return uuid
    },

    __insert__(data){ throw "not configured yet" },

    __delete__(id){ throw "not configured yet" },

    __update__(id, data){ throw "not configured yet" },

    __findById__(id){ throw "not configured yet" },

    insert(uuid, data){
        const transaction = findTransaction(uuid)
        this.__insert__(data).then( id => {
            transaction.tasks.push({ 
                name: 'insert', 
                rollback: () => {
                    this.__delete__(id)
                }
            })
        })
    },

    delete(uuid, id){
        const transaction = findTransaction(uuid)
        this.__findById__(id).then(data => {
            this.__delete__(id).then( () => {
                transaction.tasks.push({ 
                    name: 'delete', 
                    rollback: () => {
                        this.__insert__(data)
                    }
                })
            })
        })
    },

    update(uuid, id, newData){
        const transaction = findTransaction(uuid)
        this.__findById__(id).then(oldData => {
            this.__update__(id, newData).then( () => {
                transaction.tasks.push({ 
                    name: 'update', 
                    rollback: () => {
                        this.__update__(id, oldData)
                    }
                })
            })
        })
    },

    commit(uuid){
        removeTransaction(uuid)
    },

    rollback(uuid){
        const transaction = findTransaction(uuid)
        transaction.tasks.forEach(task => {
            task.rollback()
        });
        removeTransaction(uuid)
    }
}, 7306)