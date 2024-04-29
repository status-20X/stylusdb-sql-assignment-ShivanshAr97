const { parseQuery } = require('./queryParser');
const readCSV = require('./csvReader');

async function performInnerJoin(data, joinData, joinCondition, fields, table) {
    return (data = data.flatMap((mainRow) => {
        return joinData
            .filter((joinRow) => {
                const mainValue = mainRow[joinCondition.left.split(".")[1]];
                const joinValue = joinRow[joinCondition.right.split(".")[1]];
                return mainValue === joinValue;
            })
            .map((joinRow) => {
                return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split(".");
                    acc[field] =
                        tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                    return acc;
                }, {});
            });
    }));
}
async function performLeftJoin(data, joinData, joinCondition, fields, table) {
    return (data = data.flatMap((mainRow) => {
        const matchingJoinRows = joinData.filter((joinRow) => {
            const mainValue = mainRow[joinCondition.left.split(".")[1]];
            const joinValue = joinRow[joinCondition.right.split(".")[1]];
            return mainValue === joinValue;
        });

        if (matchingJoinRows.length === 0) {
            return {
                ...mainRow,
                ...fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split(".");
                    acc[field] = tableName === table ? mainRow[fieldName] : null;
                    return acc;
                }, {}),
            };
        }

        return matchingJoinRows.map((joinRow) => {
            return fields.reduce((acc, field) => {
                const [tableName, fieldName] = field.split(".");
                acc[field] =
                    tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                return acc;
            }, {});
        });
    }));
}

async function performRightJoin(data, joinData, joinCondition, fields, table) {
    return (joinData = joinData.flatMap((joinRow) => {
        const matchingDataRows = data.filter((mainRow) => {
            const mainValue = mainRow[joinCondition.left.split(".")[1]];
            const joinValue = joinRow[joinCondition.right.split(".")[1]];
            return mainValue === joinValue;
        });

        if (matchingDataRows.length === 0) {
            return {
                ...joinRow,
                ...fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split(".");
                    acc[field] = tableName === table ? null : joinRow[fieldName];
                    return acc;
                }, {}),
            };
        }

        return matchingDataRows.map((mainRow) => {
            return fields.reduce((acc, field) => {
                const [tableName, fieldName] = field.split(".");
                acc[field] =
                    tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                return acc;
            }, {});
        });
    }));
}
async function CREATEQuery(query) {
    const { fields, table, whereClauses, joinType, joinTable, joinCondition } =
        parseQuery(query);

    let data = await readCSV(`${table}.csv`);

    if (joinTable && joinCondition) {
        const joinData = await readCSV(`${joinTable}.csv`);
        switch (joinType.toUpperCase()) {
            case "INNER":
                data = await performInnerJoin(
                    data,
                    joinData,
                    joinCondition,
                    fields,
                    table
                );
                break;
            case "LEFT":
                data = await performLeftJoin(
                    data,
                    joinData,
                    joinCondition,
                    fields,
                    table
                );
                break;
            case "RIGHT":
                data = await performRightJoin(
                    data,
                    joinData,
                    joinCondition,
                    fields,
                    table
                );
                break;
        }
    }

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