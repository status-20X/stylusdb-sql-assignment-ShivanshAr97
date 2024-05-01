const child_process = require('child_process');
const path = require('path');

test('DISTINCT with Multiple Columns via CLI', (done) => {
    const cliPath = path.join(__dirname, '..', 'src', 'cli.js');
    const cliProcess = child_process.spawn('node', [cliPath]);

    let outputData = "";
    cliProcess.stdout.on('data', (data) => {
        outputData += data.toString();
    });

    cliProcess.on('exit', () => {
        const cleanedOutput = outputData.replace(/\s+/g, ' ');
        const resultRegex = /Result: (\[.+\])/s;
        const match = cleanedOutput.match(resultRegex);
        match[1] = match[1].replace(/'/g, '"').replace(/(\w+):/g, '"$1":');

        if (match && match[1]) {
            const results = JSON.parse(match[1]);
            expect(results).toEqual([
                { student_id: '1', course: 'Mathematics' },
                { student_id: '1', course: 'Physics' },
                { student_id: '2', course: 'Chemistry' },
                { student_id: '3', course: 'Mathematics' },
                { student_id: '5', course: 'Biology' },
                { student_id: '5', course: 'Physics' }
            ]);
            console.log("Test passed successfully");
        } else {
            throw new Error('Failed to parse CLI output');
        }

        done();
    });
    setTimeout(() => {
        cliProcess.stdin.write("SELECT DISTINCT student_id, course FROM enrollment\n");
        setTimeout(() => {
            cliProcess.stdin.write("exit\n");
        }, 1000);
    }, 1000);
});