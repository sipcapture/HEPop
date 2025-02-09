/*
 * Take the input value and output it as a string.
 * If the input value is numeric, look at the given numeric field type.
 * If it is 'int' output it as a line protocol int (i.e. 1234i)
 * Otherwise, output it as a float.
 */
function formatValue(v, numericType) {
    if (typeof v === 'number') {
      if (numericType == "int") {
        return `${Math.round(v)}i`;
      } else if (numericType == "float") {
        return String(v);
      } else {
        return String(v);
      }
    } else if (typeof v === 'boolean') {
      return v ? 'TRUE' : 'FALSE';
    } else {
      return JSON.stringify(v);
    }
  }
  
  function formatDate(date) {
    return (date instanceof Date ? date.getTime() : date) * 1000000;
  }
  
  const INT_REGEX = /^\d+i$/;
  const TRUE_REGEX = /^(t|true)$/i;
  const FALSE_REGEX = /^(f|false)$/i;
  const STRING_REGEX = /^"(.*)"$/;
  
  function parseValue(value) {
    if (value == null) {
      return undefined;
    } else if (INT_REGEX.test(value)) {
      return parseInt(value.slice(0, -1));
    } else if (TRUE_REGEX.test(value)) {
      return true;
    } else if (FALSE_REGEX.test(value)) {
      return false;
    } else if (STRING_REGEX.test(value)) {
      return value.slice(1, -1);
    } else if (!isNaN(value)) {
      return parseFloat(value);
    } else {
      return undefined;
    }
  }
  
  function joinObject(obj, withFormatting, config) {
    if (!obj) return '';
  
    return Object.keys(obj)
      .map(key => {
        let override = config.typeMappings.find(i => i.fieldName == key);
        let numType = override?.fieldType || config.defaultTypeMapping;
        return `${key}=${withFormatting ? formatValue(obj[key], numType) : obj[key]}`;
      })
      .join(',');
  }
  
  function parse(point, config) {
    const result = {};
  
    const [tags_, fields_, timestamp] = point.split(' ');
  
    const tags = (tags_ || '').split(',');
    const fields = (fields_ || '').split(',');
  
    result.measurement = tags.shift();
  
    result.tags = tags.reduce((out, tag) => {
      if (!tag) return out;
      var [key, value] = tag.split('=');
      out[key] = value;
      return out;
    }, {});
  
    result.fields = fields.reduce((out, field) => {
      if (!field) return out;
      var [key, value] = field.split('=');
      out[key] = parseValue(value);
      return out;
    }, {});
  
    if (timestamp) {
      result.timestamp = parseInt(timestamp) / 1000000;
    } else if (config.addTimestamp) {
      result.timestamp = Date.now();
    }
  
    return result;
  }
  
  function format(pointJson, config) {
    const { measurement, tags, fields, timestamp } = pointJson;
  
    var str = measurement;
  
    const tagsStr = joinObject(tags, false, config);
    if (tagsStr) {
      str += ',' + tagsStr;
    }
  
    str += ' ' + joinObject(fields, true, config);
  
    if (timestamp) {
      str += ' ' + formatDate(timestamp);
    } else if (config.addTimestamp) {
      str += ' ' + formatDate(new Date());
    }
  
    return str;
  }
  
  function transform(item, config) {
    if (item == null) {
      return item;
    } else if (typeof item === 'string') {
      return parse(item, config);
    } else if (typeof item === 'object' && 'measurement' in item) {
      return format(item, config);
    } else {
      return item;
    }
  }
  
  function transformArray(itemOrArray, config) {
    if (itemOrArray && Array.isArray(itemOrArray)) {
      return itemOrArray.map(item => transform(item, config));
    } else {
      return transform(itemOrArray, config);
    }
  }
  
  export {
    formatValue,
    formatDate,
    parseValue,
    joinObject,
    parse,
    format,
    transform,
    transformArray,
  };