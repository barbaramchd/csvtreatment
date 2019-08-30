const fastCsv = require('fast-csv');
const {Pool} = require('pg');
const path = require('path');
const appDir = path.dirname(require.main.filename);
const fs = require('fs');

const strConn = 'postgresql://postgres:postgres@localhost:5432/MapaDoMaroto';
const conn = new Pool({connectionString: strConn, ssl:false});

String.prototype.leticiaTrollando = function(search, replacement) {
    var target = this;
    return target.split(search).join(replacement);
};

function queryBuilder(row) {
    let query = 'INSERT INTO churn (';
    let values = ' VALUES (';
    console.log()
    Object.keys(row).forEach((coluna,index) =>{
        if(coluna.trim() !== ''){
            if(index !== Object.keys(row).length - 1) {
                query += coluna + ' , ';
                values += "'" + row[coluna].trim().leticiaTrollando('"','').replace(',','.') + "',";
            }
            else {
                query += coluna + ')';
                values += "'" + row[coluna].trim().leticiaTrollando('"','').replace(',','.') + "');";
            }
        }
    });
    return query + values;
}

function readCsvStream(filePath) {
   return new Promise(((resolve, reject) => {
       const fileStream = fs.createReadStream(filePath);
       const promisesArray = [];
       const parser = fastCsv.parse({ headers: true, delimiter: ';' });
       fileStream
           .pipe(parser)
           .on('error', (error) => {
               console.error(error);
               reject(error);
           })
           .on('readable', () => {
               for (let row = parser.read(); row; row = parser.read()) {
                   const query = queryBuilder(row);
                   promisesArray.push(conn.query(query));
               }
           })
           .on('end', () => {
               resolve(promisesArray);
           });
   }));
}
async function lerCsv(){
    const promisesArray = await readCsvStream(`${appDir}/churn (3).csv`);
    await Promise.all(promisesArray);
    console.log('Parabens barbara!');
}

lerCsv();