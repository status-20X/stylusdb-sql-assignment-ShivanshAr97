function parseQuery(query) {
    try {
        query = query.trim();
        let isDistinct = false;
        if (query.toUpperCase().includes('SELECT DISTINCT')) {
            isDistinct = true;
            query = query.replace('SELECT DISTINCT', 'SELECT');
        }

        const limitRegex = /\sLIMIT\s(\d+)/i;
        const limitMatch = query.match(limitRegex);
        let limit = null;
        if (limitMatch) {
            limit = parseInt(limitMatch[1], 10);
            query = query.replace(limitRegex, '');
        }
        const orderByRegex = /\sORDER BY\s(.+)/i;
        const orderByMatch = query.match(orderByRegex);
        let orderByFields = null;
        if (orderByMatch) {
            orderByFields = orderByMatch[1].split(',').map(field => {
                const [fieldName, order] = field.trim().split(/\s+/);
                return { fieldName, order: order ? order.toUpperCase() : 'ASC' };
            });
            query = query.replace(orderByRegex, '');
        }
        const groupByRegex = /\sGROUP BY\s(.+)/i;
        const groupByMatch = query.match(groupByRegex);
        let groupByFields = null;
        if (groupByMatch) {
            groupByFields = groupByMatch[1].split(',').map(field => field.trim());
            query = query.replace(groupByRegex, '');
        }
        const whereSplit = query.split(/\sWHERE\s/i);
        const queryWithoutWhere = whereSplit[0];
        const whereClause = whereSplit.length > 1 ? whereSplit[1].trim() : null;
        const joinSplit = queryWithoutWhere.split(/\s(INNER|LEFT|RIGHT) JOIN\s/i);
        const selectPart = joinSplit[0].trim();
        const { joinType, joinTable, joinCondition } = parseJoinClause(queryWithoutWhere);
        const selectRegex = /^SELECT\s(.+?)\sFROM\s(.+)/i;
        const selectMatch = selectPart.match(selectRegex);
        if (!selectMatch) {
            throw new Error('Invalid SELECT format');
        }
        const [, fields, table] = selectMatch;
        let whereClauses = [];
        if (whereClause) {
            whereClauses = parseWhereClause(whereClause);
        }
        const hasAggregateWithoutGroupBy = checkAggregateWithoutGroupBy(query, groupByFields);
        return {
            fields: fields.split(',').map(field => field.trim()),
            table: table.trim(),
            whereClauses,

            orderByFields,
            joinType,
            joinTable,
            joinCondition,
            groupByFields,
            hasAggregateWithoutGroupBy,
            limit,
            isDistinct
        };
    } catch (error) {
        throw new Error(`Query parsing error: ${error.message}`);
    }
}

function checkAggregateWithoutGroupBy(query, groupByFields) {
    const aggregateFunctionRegex = /(\bCOUNT\b|\bAVG\b|\bSUM\b|\bMIN\b|\bMAX\b)\s*\(\s*(\*|\w+)\s*\)/i;
    return aggregateFunctionRegex.test(query) && !groupByFields;
}

function parseWhereClause(whereString) {
    const conditionRegex = /(.*?)(=|!=|>|<|>=|<=)(.*)/;
    return whereString.split(/ AND | OR /i).map(conditionString => {
        if (conditionString.includes(' LIKE ')) {
            const [field, pattern] = conditionString.split(/\sLIKE\s/i);
            return { field: field.trim(), operator: 'LIKE', value: pattern.trim().replace(/^'(.*)'$/, '$1') };
        } else {
            const match = conditionString.match(conditionRegex);
            if (match) {
                const [, field, operator, value] = match;
                return { field: field.trim(), operator, value: value.trim() };
            }
            throw new Error('Invalid WHERE clause format');
        }
        throw new Error('Invalid WHERE clause format');
    });
}
function parseJoinClause(query) {
    const joinRegex = /\s(INNER|LEFT|RIGHT) JOIN\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
    const joinMatch = query.match(joinRegex);

    if (joinMatch) {
        return {
            joinType: joinMatch[1].trim(),
            joinTable: joinMatch[2].trim(),
            joinCondition: {
                left: joinMatch[3].trim(),
                right: joinMatch[4].trim()
            }
        };
    }
    return {

        joinCondition: null,
        joinType: null,
        joinTable: null,
    };
}

function parseInsertQuery(query) {
    const insertRegex = /INSERT INTO (\w+)\s\((.+)\)\sVALUES\s\((.+)\)/i;
    const match = query.match(insertRegex);

    if (!match) {
        throw new Error("Invalid INSERT INTO syntax.");
    }

    const [, table, columns, values] = match;
    return {
        type: 'INSERT',
        table: table.trim(),
        columns: columns.split(',').map(column => column.trim()),
        values: values.split(',').map(value => value.trim())
    };
}

function parseDeleteQuery(query) {
    const deleteRegex = /DELETE FROM (\w+)( WHERE (.*))?/i;
    const match = query.match(deleteRegex);

    if (!match) {
        throw new Error("Invalid DELETE syntax.");
    }

    const [, table, , whereString] = match;
    let whereClauses = [];
    if (whereString) {
        whereClauses = parseWhereClause(whereString);
    }

    return {
        type: 'DELETE',
        table: table.trim(),
        whereClauses
    };
}

module.exports = { parseQuery, parseJoinClause, parseInsertQuery, parseDeleteQuery };