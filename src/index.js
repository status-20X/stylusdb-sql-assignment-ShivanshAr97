const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');

async function CREATEQuery(query) {
    const { fields, table, whereClauses } = parseQuery(query);
    const data = await readCSV(`${table}.csv`);
    const filterData = () => {
        return whereClauses.length > 0
            ? data.filter((row) =>
                whereClauses.every((clause) => evaluateCondition(row, clause))
            )
            : data;
    };
    const filteredData = filterData();

    try {
        return filteredData.map((row) => {
            const selectedRow = {};
            fields.forEach((field) => {
                selectedRow[field] = row[field];
            });
            return selectedRow;
        });
    } catch (err) {
        throw new Error(
            "Fields in query doesn't match the fields in filtered data"
        );
    }
}

function evaluateCondition(row, clause) {
    const { field, operator, value } = clause;
    switch (operator) {
        case "=":
            return row[field] === value;
        case "!=":
            return row[field] !== value;
        case ">":
            return row[field] > value;
        case "<":
            return row[field] < value;
        case ">=":
            return row[field] >= value;
        case "<=":
            return row[field] <= value;
        default:
            throw new Error(`Unsupported operator: ${operator}`);
    }
}

module.exports = CREATEQuery;