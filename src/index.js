const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');

// async function CREATEQuery(query) {
//     const { fields, table } = parseQuery(query);
//     const data = await readCSV(`${table}.csv`);
//     return data.map(row => {
//         const filteredRow = {};
//         fields.forEach(field => {
//             filteredRow[field] = row[field];
//         });
//         return filteredRow;
//     });
// }

// async function CREATEQuery(query) {
//     const { fields, table, whereClause } = parseQuery(query);
//     const data = await readCSV(`${table}.csv`);
//     const filteredData = whereClause
//         ? data.filter(row => {
//             const [field, value] = whereClause.split('=').map(s => s.trim());
//             return row[field] === value;
//         })
//         : data;
//     return filteredData.map(row => {
//         const selectedRow = {};
//         fields.forEach(field => {
//             selectedRow[field] = row[field];
//         });
//         return selectedRow;
//     });
// }

// src/index.js

async function CREATEQuery(query) {
    const { fields, table, whereClauses } = parseQuery(query);
    const data = await readCSV(`${table}.csv`);
    const filteredData = whereClauses.length > 0
        ? data.filter(row => whereClauses.every(clause => {
            return row[clause.field] === clause.value;
        }))
        : data;

    return filteredData.map(row => {
        const selectedRow = {};
        fields.forEach(field => {
            selectedRow[field] = row[field];
        });
        return selectedRow;
    });
}

module.exports = CREATEQuery;