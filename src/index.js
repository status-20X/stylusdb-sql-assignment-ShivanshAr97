const { parseQuery } = require("./queryParser");
const readCSV = require("./csvReader");

async function CREATEQuery(query) {
    const {
        fields,
        table,
        whereClauses,
        joinType,
        joinTable,
        joinCondition,
        groupByFields,
        hasAggregateWithoutGroupBy,
    } = parseQuery(query);
    let data = await readCSV(`${table}.csv`);

    console.log({
        fields,
        table,
        whereClauses,
        joinType,
        joinTable,
        joinCondition,
        groupByFields,
        hasAggregateWithoutGroupBy,
        data,
    });

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
        if (whereClauses.length > 0) {
            return data.filter((row) => {
                return whereClauses.every((clause) => {
                    return evaluateCondition(row, clause);
                });
            });
        } else {
            return data;
        }
    };
    let filteredData;
    filteredData = filterData();

    console.log("filtered data is ", filteredData);

    if (groupByFields || hasAggregateWithoutGroupBy) {
        const aggregateFields = fields
            .map((field) => getAggregateFields(field))
            .filter((field) => field != undefined);
        console.log("Aggregate fields are ", aggregateFields);
        filteredData = applyGroupBy(
            filteredData,
            groupByFields,
            aggregateFields,
            hasAggregateWithoutGroupBy
        );
    }

    try {
        if (fields[0] === "*") {
            return filterData;
        }
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

async function performInnerJoin(data, joinData, joinCondition, fields, table) {
    return (data = data.flatMap((mainRow) => {
        return joinData
            .filter((joinRow) => {
                const mainValue = mainRow[joinCondition.left.split(".")[1]];
                const joinValue = joinRow[joinCondition.right.split(".")[1]];
                return mainValue === joinValue;
            })
            .map((joinRow) => {
                // Constructing output object with prefixed table name for keys
                let output = {};
                for (const key in mainRow) {
                    output[table + "." + key] = mainRow[key];
                }
                for (const key in joinRow) {
                    output[joinCondition.right.split(".")[0] + "." + key] = joinRow[key];
                }
                // Retain only the specified fields
                const filteredOutput = {};
                fields.forEach((field) => {
                    const [tableName, fieldName] = field.split(".");
                    filteredOutput[field] =
                        tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                });
                return { ...output, ...filteredOutput }; // Merging prefixed keys with specified fields
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
            // Constructing output object with prefixed table name for keys
            let output = {};
            for (const key in mainRow) {
                output[table + "." + key] = mainRow[key];
            }
            // Retain only the specified fields
            const filteredOutput = {};
            fields.forEach((field) => {
                const [tableName, fieldName] = field.split(".");
                filteredOutput[field] = tableName === table ? mainRow[fieldName] : null;
            });
            return { ...output, ...filteredOutput }; // Merging prefixed keys with specified fields
        }

        return matchingJoinRows.map((joinRow) => {
            // Constructing output object with prefixed table name for keys
            let output = {};
            for (const key in mainRow) {
                output[table + "." + key] = mainRow[key];
            }
            for (const key in joinRow) {
                output[joinCondition.right.split(".")[0] + "." + key] = joinRow[key];
            }
            // Retain only the specified fields
            const filteredOutput = {};
            fields.forEach((field) => {
                const [tableName, fieldName] = field.split(".");
                filteredOutput[field] =
                    tableName === table ? mainRow[fieldName] : joinRow[fieldName];
            });
            return { ...output, ...filteredOutput }; // Merging prefixed keys with specified fields
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
            // Constructing output object with prefixed table name for keys
            let output = {};
            for (const key in joinRow) {
                output[joinCondition.right.split(".")[0] + "." + key] = joinRow[key];
            }
            // Retain only the specified fields
            const filteredOutput = {};
            fields.forEach((field) => {
                const [tableName, fieldName] = field.split(".");
                filteredOutput[field] = tableName === table ? null : joinRow[fieldName];
            });
            return { ...output, ...filteredOutput }; // Merging prefixed keys with specified fields
        }

        return matchingDataRows.map((mainRow) => {
            // Constructing output object with prefixed table name for keys
            let output = {};
            for (const key in mainRow) {
                output[table + "." + key] = mainRow[key];
            }
            for (const key in joinRow) {
                output[joinCondition.right.split(".")[0] + "." + key] = joinRow[key];
            }
            // Retain only the specified fields
            const filteredOutput = {};
            fields.forEach((field) => {
                const [tableName, fieldName] = field.split(".");
                filteredOutput[field] =
                    tableName === table ? mainRow[fieldName] : joinRow[fieldName];
            });
            return { ...output, ...filteredOutput }; // Merging prefixed keys with specified fields
        });
    }));
}

function evaluateCondition(row, clause) {
    let { field, operator, value } = clause;
    value = trimQuotes(value);
    if (row[field] === undefined) return true;
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

function trimQuotes(inputString) {
    return inputString.replace(/^['"]|['"]$/g, "");
}

function getAggregateFields(field) {
    const match = field.match(/^(AVG|SUM|COUNT|MIN|MAX)\((.+)\)/i);
    console.log("match is ", match);
    console.log("match is ", match);
    if (match) {
        return { function: match[1].trim(), on: match[2].trim() };
    }
}

function applyGroupBy(
    data,
    groupByFields,
    aggregateFields,
    hasAggregateWithoutGroupBy
) {
    if (!aggregateFields.length) {
        return data;
    }
    if (!groupByFields && hasAggregateWithoutGroupBy) {
        const aggregatedValues = {};
        aggregateFields.forEach((field) => {
            const { function: aggFunction, on: fieldToAggregate } = field;
            switch (aggFunction.toUpperCase()) {
                case "COUNT":
                    aggregatedValues[aggFunction + "(" + fieldToAggregate + ")"] =
                        data.length;
                    break;
                case "SUM":
                    const sumin = data.reduce(
                        (acc, row) => acc + Number(row[fieldToAggregate]),
                        0
                    );
                    aggregatedValues[aggFunction + "(" + fieldToAggregate + ")"] = sumin;
                    break;
                case "MIN":
                    aggregatedValues[aggFunction + "(" + fieldToAggregate + ")"] =
                        Math.min(...data.map((row) => row[fieldToAggregate]));
                    break;
                case "MAX":
                    aggregatedValues[aggFunction + "(" + fieldToAggregate + ")"] =
                        Math.max(...data.map((row) => row[fieldToAggregate]));
                    break;
                case "AVG":
                    const sum = data.reduce(
                        (acc, row) => acc + Number(row[fieldToAggregate]),
                        0
                    );
                    const count = data.length;
                    aggregatedValues[aggFunction + "(" + fieldToAggregate + ")"] =
                        count > 0 ? sum / count : null;
                    break;
            }
        });
        return [aggregatedValues];
    }

    const groupMap = new Map();

    data.forEach((row) => {
        const groupKey = groupByFields
            ? groupByFields.map((field) => row[field]).join("_")
            : "";
        const group = groupMap.get(groupKey) || { rows: [], count: 0 };
        group.rows.push(row);
        group.count++;
        groupMap.set(groupKey, group);
    });

    const results = [];
    for (const [groupKey, group] of groupMap.entries()) {
        const aggregatedValues = {};
        aggregateFields.forEach((field) => {
            const { function: aggFunction, on: fieldToAggregate } = field;
            switch (aggFunction.toUpperCase()) {
                case "COUNT":
                    aggregatedValues[aggFunction + "(" + fieldToAggregate + ")"] =
                        group.count;
                    break;
                case "SUM":
                    const initialValue =
                        group.rows.length > 0 ? Number(group.rows[0][fieldToAggregate]) : 0;
                    aggregatedValues[aggFunction + "(" + fieldToAggregate + ")"] =
                        group.rows.reduce(
                            (acc, row) => acc + Number(row[fieldToAggregate]),
                            initialValue
                        );
                    break;
                case "MIN":
                    aggregatedValues[aggFunction + "(" + fieldToAggregate + ")"] =
                        Math.min(...group.rows.map((row) => row[fieldToAggregate]));
                    break;
                case "MAX":
                    aggregatedValues[aggFunction + "(" + fieldToAggregate + ")"] =
                        Math.max(...group.rows.map((row) => row[fieldToAggregate]));
                    break;
                case "AVG":
                    let sum = 0;
                    let count = 0;
                    for (const row of group.rows) {
                        const value = Number(row[fieldToAggregate]);
                        if (!isNaN(value)) {
                            sum += value;
                            count++;
                        }
                    }
                    aggregatedValues[aggFunction + "(" + fieldToAggregate + ")"] =
                        count > 0 ? sum / count : null;
                    break;
            }
        });

        if (groupByFields) {
            const groupRow = {};
            groupByFields.forEach(
                (field) => (groupRow[field] = group.rows[0][field])
            );
            results.push({ ...groupRow, ...aggregatedValues });
        } else {
            results.push(aggregatedValues);
        }
    }

    return results;
}

module.exports = CREATEQuery;