var cassandra = require('./node_modules/cassandra-driver'),
    Mapper = cassandra.mapping.Mapper,
    uuid = require('uuid').v4,

    Client = undefined,
    crypto = require('crypto'),
    _ = require('lodash');
const TableSchema = {};
Array.prototype.compareObject = function (obj) {
    var i = this.length;
    var obj = JSON.stringify(obj);
    while (i--) {
        if (JSON.stringify(this[i]) === obj) {
            return true;
        }
    }
    return false;
}
Array.prototype.sorted = function (cond) {
    // console.log(cond);
    if ('created') {
        return this.sort(function (obj1, obj2) {
            return new Date(obj2[cond]) - new Date(obj1[cond]);
        })
    }


}
module.exports = function (clientObject,callback) {
    var client = cassandra.Client;
    if (clientObject.user && clientObject.password) {
        if (clientObject.auth) {
            clientObject.authProvider = new cassandra.auth[clientObject.auth](String(clientObject.user), String(clientObject.password));
            delete clientObject.auth;
        }
        else {
            clientObject.authProvider = new cassandra.auth.PlainTextAuthProvider(String(clientObject.user), String(clientObject.password));
        }
        delete clientObject.user;
        delete clientObject.password;

    }
    // console.log(clientObject);
    try {
        Client = new client(clientObject)
        Client.connect(function(err){ 
            if(err){
                console.log(err);
                process.exit();
            }else{
                callback(Client)
            }
            
        })
        // return new Promise((accept, reject) => {
        //     accept(Client);
        // })
    } catch (err) {
        console.log(err);
        process.exit();
        // throw new Error(err)
        
    }

    
}

function getMapperObject(table) {
    var model = {
        models: {

        }
    }
    model.models[table] = {
        tables: [table],
        // columns: Schema.col,
    }
    // console.log(model.models);

    var mapper = new Mapper(Client, model);
    return mapper.forModel(table);
}
module.exports.Schema = function (tablename, schema) {
    if (!TableSchema[tablename]) {
        var keys = Object.keys(schema);
        TableSchema[tablename] = {};
        TableSchema[tablename].col = {};
        TableSchema[tablename].schema = {};
        TableSchema[tablename].ref = {};
        // console.log(schema)
        // TableSchema[tablename]._id = uuid();
        keys.forEach((element, i) => {
            if (schema[element].columnName) {
                TableSchema[tablename].col[element] = schema[element].columnName
            }
            else {
                TableSchema[tablename].col[element] = element;
            }
            if (schema[element].default) {
                TableSchema[tablename].schema[TableSchema[tablename].col[element]] = schema[element].default;
            }
            else {
                TableSchema[tablename].schema[TableSchema[tablename].col[element]] = null;
            }
            if (schema[element].ref) {
                TableSchema[tablename].ref[element] = schema[element].ref;
            }

        })
        TableSchema[tablename].mapper = getMapperObject(tablename);
    }
    else {
        console.error(new Error('schema for this table has already been declared'))
        process.exit();


    }
}
function ConvertToPassToServer(objectToConvert, col) {
    if (_.isArray(objectToConvert)) {
        var Arr = [];
        for (i of objectToConvert) {
            Arr.push(ConvertToPassToServer(i, col));
        }
        return Arr
    }
    else {
        var keys = Object.keys(col);
        // console.log(objectToConvert.created);

        var objectToconvert = JSON.parse(JSON.stringify(objectToConvert));
        // console.log(objectToConvert);

        keys.forEach((ele, i) => {
            if (ele != col[ele]) {
                Object.defineProperty(objectToconvert, col[ele],
                    Object.getOwnPropertyDescriptor(objectToconvert, ele));
                delete objectToconvert[ele];
            }
        })

        return objectToconvert

    }
}
function beautify(object) {
    var obj = {};
    if (_.isArray(object)) {
        var arr = [];

        object.forEach((ele, i) => {
            // var copyJson = Object.assign({}, ele.object)
            var copyJson = ele
            delete copyJson.col
            // var obj={};
            var keys = Object.keys(copyJson);
            keys.forEach((ele, i) => {
                obj[ele] = copyJson[ele];
            })
            arr.push(obj)
        })
        return arr
    }
    else {
        // var copyJson = Object.assign({}, object.object)
        var copyJson = object.object;
        delete copyJson.col
        var keys = Object.keys(copyJson);
        keys.forEach((ele, i) => {
            obj[ele] = copyJson[ele];
        })
        return object.object;
    }
}
function ConvertToPassToDB() {

}
async function findOne(mapper, criteria, col) {
    // var mapper = mapper;
    var array = Object.keys(criteria);
    // console.log(criteria,'column id error');
    
    if (array.includes('_id')) {
        try {
            var value = await mapper.get({ id: criteria._id });
            if (value) {
                var output = ConvertToPassToServer(value, col);
                return output;
            }
            return null;

        }
        catch (e) {
            throw new Error(e)
        }
    }
    else {
        var query = 'Select * from ' + mapper.name + ' where ' + array[0].toLowerCase() + '=? Allow Filtering';
        mapper.usernameQuery = mapper.mapWithQuery(query, criteria => [criteria[array[0]]]);
        try {
            var res = await mapper.usernameQuery(criteria);
        }
        catch (e) {
            console.log(e, 'error');
            throw new Error(e);
        }
        if (res._rs.rows.length > 0) {
            let output = ConvertToPassToServer(res._rs.rows[0], col)
            return output;
        }
        return null;
    }

}
async function find(mapper, criteria, col) {
    // var mapper = mapper;
    var array = Object.keys(criteria);
    if (array.includes('_id')) {
        // try {
        var value = await mapper.get({ id: criteria._id });
        if (value) {
            var output = ConvertToPassToServer([value], col);
            // console.log(output);

            return output;
        }
        return null;

        // }
        // catch (e) {
        //     throw new Error(e)
        // }
    }
    else {
        if (array.length == 1) {
            var query = 'Select * from ' + mapper.name + ' where ' + array[0].toLowerCase() + '=? Allow Filtering';
            mapper.Query = mapper.mapWithQuery(query, criteria => [criteria[array[0]]]);
            try {
                var res = await mapper.Query(criteria);
            }
            catch (e) {
                console.log(e, 'error');
                throw new Error(e);
            }
            if (res._rs.rows.length > 0) {
                var arr = ConvertToPassToServer(res._rs.rows, col)
                return arr;
            }
            return null;
        }
        else {
            let query = 'Select * from ' + mapper.name + " where ";
            let valueArray = [];
            for (let i of array) {
                valueArray.push(criteria[i]);
                query += ' ' + i.toLowerCase() + ' =? and '
            }
            query = query.slice(0, -4);
            query += ' Allow Filtering';
            mapper.QueryMany = mapper.mapWithQuery(query, criteria => {
                return valueArray
            });
            try {
                var res = await mapper.QueryMany(criteria);
            }
            catch (e) {
                console.log(e, 'error');
                throw new Error(e);
            }
            if (res._rs.rows.length > 0) {
                var arr = ConvertToPassToServer(res._rs.rows, col)
                return arr;
            }
            return null;
        }

    }

}
async function Or(mapper, criteria, col) {
    var outputArr = [];
    for (let i of criteria['$or']) {
        let result = await find(mapper, i, col);
        if (result) {
            for (i of result) {

                if (!outputArr.compareObject(i)) {
                    outputArr.push(i)
                }
            }
        }
    }
    return outputArr;


}
async function update(mapper, findObj, criteria, col) {
    let res = await findOne(mapper, findObj, col)
    // console.log(res,'res');
    if (res) {
        res = ConvertToPassToServer(res, _.invert(col));
        // res = ConvertToPassToServer(res,_.invert(col));  
        let keys = Object.keys(criteria)
        keys.forEach((ele) => {
            res[ele.toLowerCase()] = criteria[ele]
        })
        if (Object.keys(res).includes('updated')) {
            res.updated = new Date().toISOString();
        }
        try {
            mapper.insert(res, { fields: Object.keys(res) }, { ifNotExists: true })
            return res;
        }
        catch (e) {
            console.log(e);
            throw new Error(e)
        }
    }
    else {
        // console.log(criteria,'res');
        let res = ConvertToPassToServer(criteria, _.invert(col));
        let keys = Object.keys(criteria)
        keys.forEach((ele) => {
            res[ele.toLowerCase()] = criteria[ele]
        })
        if (Object.keys(res).includes('updated')) {
            res.updated = new Date().toISOString();
        }
        try {
            mapper.insert(res, { fields: Object.keys(res) }, { ifNotExists: true })
            return res;
        }
        catch (e) {
            console.log(e);
            throw new Error(e)
        }
    }


}
async function findAll(mapper, col) {
    let res = await mapper.findAll();
    if (res._rs.rows.length > 0) {
        let result = ConvertToPassToServer(res._rs.rows, col);
        return result;
    }
    return null;

}


async function destroy(mapper, criteria, col) {
    let array = Object.keys(criteria);
    if (array.includes('_id')) {
        var obj = await mapper.remove({ id: criteria._id });
        obj = ConvertToPassToServer(obj, col);
        return obj;
    }
    else {
        throw new Error('Destroy cant work without id field')
    }
}

async function populate() {
    console.log('populate');

}

module.exports.model = function (table) {
    const tableObject = TableSchema[table];
    class Table {
        constructor(object) {
            this.merge(object);
            if (Object.keys(this).includes('created')) {
                // this.created = new Date().toISOString();
                if (!this.created) {
                    this.created = new Date().toISOString();
                }
            }

        }
        async save(callback) {
            try {
                var res = await update(tableObject.mapper, { _id: this._id }, this, tableObject.col)
                res = ConvertToPassToServer(res, tableObject.col);
                Object.assign(this, res)
                if (callback) {
                    callback(null)
                }
            }
            catch (err) {
                if (callback) {
                    callback(err)
                } else {
                    console.log(err);

                    throw new Error(err)
                }
            }

        }
        preSave() {
            this.salt = crypto.randomBytes(16).toString('base64');
            this.password = this.hash(this.password);
        }
        hash(password) {
            if (this.salt && password) {
                return crypto.pbkdf2Sync(password, Buffer.from(this.salt, 'base64'), 10000, 64, 'SHA1').toString('base64');
            } else {
                return password;
            }
        }
        authenticate(password) {
            return this.password == this.hash(password);
        }
        merge(object) {
            if (object) {
                if (!object._id) {
                    object._id = uuid();
                    object.created = new Date().toISOString();
                }
                var schema = JSON.parse(JSON.stringify(tableObject.schema))
                var tempObj = _.merge(schema, object);
                for (var i in tempObj) {
                    this[i] = tempObj[i];
                }
            }
            else {
                var tempObj = _.merge({}, tableObject.schema);
                for (var i in tempObj) {
                    this[i] = tempObj[i];
                }
            }

        }
        static async find(condition, callback) {
            if (condition) {
                try {
                    var res = await find(tableObject.mapper, condition, tableObject.col)
                } catch (e) {
                    if (e) {
                        if (callback) {
                            callback(e, null);
                        }
                        else {
                            return new Promise((accept, reject) => {
                                accept(e, null);
                            })
                        }

                    }
                }

                let resu = await result(res)
                if (!resu) {
                    resu = [];
                }
                if (callback) {
                    callback(null, resu);
                }
                else {
                    return resu;
                }
            }
            else {
                var res = await findAll(tableObject.mapper, tableObject.col);
                let resu = result(res);
                if (!resu) {
                    resu = [];
                }
                if (callback) {
                    callback(null, resu);
                }
                else {
                    return resu;
                }
            }



        }
        static async findOne(condition) {
            var res = await findOne(tableObject.mapper, condition, tableObject.col)
            return result(res);
        }
        static async update(objectToUpdate, condition) {
            var result = update(tableObject.mapper, objectToUpdate, condition, tableObject.col);
            return result;
        }
        static async Or(condition) {
            let res = await Or(tableObject.mapper, condition, tableObject.col);
            return result(res)
        }
        static async findById(condition) {
            var res = await findOne(tableObject.mapper, { _id: condition }, tableObject.col)
            return result(res);
        }
        static async destroy(condition, callback) {
            try {
                let res = await destroy(tableObject.mapper, condition, tableObject.col)
                if (res) {
                    callback(undefined, res)
                } else {
                    throw new Error('couldnt delete the specifed row')
                }
            } catch (e) {
                callback(e, undefined)
            }


        }
        async populate(key, field) {
            let refTable = tableObject.ref[key];
            if (refTable) {
                let refTablObject = TableSchema[refTable];
                if (refTablObject) {
                    var variable = this[key]
                    if (_.isArray(variable)) {
                        let temArray = [];
                        for (let i of variable) {
                            let res = await findOne(refTablObject.mapper, { _id: i }, refTablObject.col)
                            let resu = await result(res);
                            if (resu) {
                                if (field) {
                                    let obj = {
                                        _id: resu._id,
                                    }
                                    // console.log(Object.getOwnPropertyDescriptor(resu, field));
                                    // break
                                    Object.defineProperty(obj, field,
                                        Object.getOwnPropertyDescriptor(resu,field));
                                    // delete resu;
                                    temArray.push(obj);
                                } else {
                                    temArray.push(resu)
                                }
                            }
                            else {
                                throw new Error('couldnt populate the object as there was no obj in DB on the referenced table')
                            }

                        }
                        this[key] = temArray;
                        // console.log(this,'populate Array');
                        
                    } else {
                        var res = await findOne(refTablObject.mapper, { _id: variable }, refTablObject.col)
                        let resu = await result(res);
                        if (resu) {
                            if (field) {
                                let obj = {
                                    _id: resu._id,
                                }
                                Object.defineProperty(obj, field,
                                    Object.getOwnPropertyDescriptor(resu, resu[field]));
                                this[key] = obj;
                            } else {
                                this[key] = resu;
                            }
                        } else {
                            throw new Error('couldnt populate the object as there was no obj in DB on the referenced table');
                        }
                    }

                    // let res = findOne({:})
                } else {
                    throw new Error('no model found for the referenced table')
                }


            } else {
                throw new Error('The column:' + key + ' to populate has no reference table')
            }
        }

    }

    return Table
    function result(result) {
        if (result && _.isArray(result)) {
            let res = new Array();
            result.forEach((ele) => {
                res.push(new Table(JSON.parse(JSON.stringify(ele))));
            })
            return res;

        }
        else if (result && !_.isArray(result)) {
            return new Table(result)
        }
        else {
            return new Promise((accept, reject) => {
                accept(null)
            })
        }

    }
}


// module.exports.Schema = class Schema {
//     constructor(SchemaObject) {
// var keys = Object.keys(SchemaObject);
// this.col = {
//     id: '_id',
// };
// this._id = uuid();
// keys.forEach((element, i) => {
//     if (SchemaObject[element].columnName) {
//         this.col[element] = SchemaObject[element].columnName
//     }
//     else {
//         this.col[element] = element;
//     }
//     if (SchemaObject[element].default) {
//         this[this.col[element]] = element.default;
//     }
//     else {
//         this[this.col[element]] = null;
//     }

// })
//         this.ass()
//     }
//     ass(){
//         console.log(this);

//     }
// }
// var TableMapper = {};
// module.exports.SaveModel = function (table, Schema) {
//     if (!Client) {
//         throw new Error('Please pass a connection to cassandra-mapper\n Hint:Pass a client object to connect');
//     }
//     function getMapperObject(table) {
//         var model = {
//             models: {

//             }
//         }
//         model.models[table] = {
//             tables: [table],
//             // columns: Schema.col,
//         }
//         // console.log(model.models);

//         var mapper = new Mapper(Client, model);
//         return mapper;
//     }
//     // delete Schema.col;
//     // console.log(Schema);

//     TableMapper[table] = { mapper: getMapperObject(table).forModel(table), Schema: Schema };
// }


// module.exports.model = function (table) {
//     var mapper = TableMapper[table].mapper;


//     function ConvertToPassToServer(objectToConvert, type) {
//         if (type === 'array') {
//             // console.log(objectToConvert.object,'object');

//             var keys = Object.keys(objectToConvert.object.col);
//             // objectToConvert.object.id = objectToConvert.object._id;
//             keys.forEach((ele, i) => {
//                 if (ele != objectToConvert.object.col[ele]) {
//                     Object.defineProperty(objectToConvert.object, objectToConvert.object.col[ele],
//                         Object.getOwnPropertyDescriptor(objectToConvert.object, ele));
//                     delete objectToConvert.object[ele];
//                 }
//             })

//             return objectToConvert

//         }
//         else {
//             // console.log(objectToConvert.object,'object');

//             var keys = Object.keys(objectToConvert.object.col);
//             // objectToConvert.object.id = objectToConvert.object._id;
//             keys.forEach((ele, i) => {
//                 if (ele != objectToConvert.object.col[ele]) {
//                     Object.defineProperty(objectToConvert.object, objectToConvert.object.col[ele],
//                         Object.getOwnPropertyDescriptor(objectToConvert.object, ele));
//                     delete objectToConvert.object[ele];
//                 }
//             })

//         }
//     }
//     function beautify(object) {
//         var obj = {};
//         if (_.isArray(object)) {
//             var arr = [];

//             object.forEach((ele, i) => {
//                 // var copyJson = Object.assign({}, ele.object)
//                 var copyJson = ele
//                 delete copyJson.col
//                 // var obj={};
//                 var keys = Object.keys(copyJson);
//                 keys.forEach((ele, i) => {
//                     obj[ele] = copyJson[ele];
//                 })
//                 arr.push(obj)
//             })
//             return arr
//         }
//         else {
//             // var copyJson = Object.assign({}, object.object)
//             var copyJson = object.object;
//             delete copyJson.col
//             var keys = Object.keys(copyJson);
//             keys.forEach((ele, i) => {
//                 obj[ele] = copyJson[ele];
//             })
//             return object.object;
//         }
//     }
//     function ConvertToPassToDB() {

//     }
//     class Table {
//         constructor(object) {
//             this.object = _.merge(TableMapper[table].Schema, object);
//             this.ass();
//         }
//         static changekeys(object) {
//             var keys = Object.keys(this.object.col);
//             keys.forEach((ele, i) => {
//                 if (ele != this.object.col[ele]) {
//                     Object.defineProperty(object, this.object.col[ele],
//                         Object.getOwnPropertyDescriptor(object, ele));
//                     delete object[ele];
//                 }
//             })
//         }
//         static async findOne(criteria) {
//             var array = Object.keys(criteria);
//             if (array.includes('_id')) {
//                 try {
//                     var value = await mapper.get({ id: criteria._id });
//                     value = new Table(value);
//                     ConvertToPassToServer(value)

//                 }
//                 catch (e) {
//                     throw new Error(e)
//                 }
//                 return new Promise((accept, reject) => {
//                     accept(beautify(value));
//                 })
//             }
//             else {
//                 var query = 'Select * from users where ' + array[0] + '=? Allow Filtering';
//                 mapper.usernameQuery = mapper.mapWithQuery(query, criteria => [criteria[array[0]]]);
//                 try {
//                     var res = await mapper.usernameQuery(criteria);
//                 }
//                 catch (e) {
//                     console.log(e);
//                     throw new Error(e);
//                 }
//                 if (res._rs.rows[0]) {
//                     var value = new Table(res._rs.rows[0]);
//                     // Table.changekeys(res._rs.rows[0]);
//                     // console.log(res._rs.rows[0]);
//                     ConvertToPassToServer(value)
//                     return new Promise((accept, reject) => {
//                         accept(beautify(value))
//                     })
//                 }
//                 else {
//                     return new Promise((accept, reject) => {
//                         reject(null)
//                     })
//                 }
//                 // console.log(res._rs.rows[0]);

//                 // this.changekeys(res._rs.rows[0])

//             }

//         }
//         static async find(criteria) {
//             var array = Object.keys(criteria);
//             if (array.includes('_id')) {
//                 try {
//                     var value = await mapper.get({ id: criteria._id });
//                     value = new Table(value);
//                     ConvertToPassToServer(value)

//                 }
//                 catch (e) {
//                     throw new Error(e)
//                 }
//                 return new Promise((accept, reject) => {
//                     accept(beautify(value));
//                 })
//             }
//             else {
//                 var query = 'Select * from users where ' + array[0] + '=? Allow Filtering';
//                 mapper.usernameQuery = mapper.mapWithQuery(query, criteria => [criteria[array[0]]]);
//                 try {
//                     var res = await mapper.usernameQuery(criteria);
//                 }
//                 catch (e) {
//                     console.log(e);
//                     throw new Error(e);
//                 }
//                 if (res._rs.rows.length > 0) {
//                     var promiseArr = [];
//                     res._rs.rows.forEach((ele, i) => {
//                         var value = new Table(ele)
//                         promiseArr.push(beautify(ConvertToPassToServer(JSON.parse(JSON.stringify(value)), 'array')));
//                     })


//                     return new Promise((accept, reject) => {
//                         accept(promiseArr)
//                     })
//                 }
//                 else {
//                     return new Promise((accept, reject) => {
//                         reject(null)
//                     })
//                 }
//             }

//         }
//         ass() {

//         }




//     }





//     return Table
// }

