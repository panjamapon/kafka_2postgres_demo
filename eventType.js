const avro = require('avsc');
const type = avro.Type.forValue({
    city: 'Cambridge',
    zipCodes: ['02138', '02139'],
    visits: 2
});
console.log(type)