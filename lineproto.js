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
  
  function parse(line, config = {}) {
    // Extract measurement and tags
    const spaceIndex = line.indexOf(' ');
    if (spaceIndex === -1) throw new Error('Invalid line protocol format');
    
    const measurementAndTags = line.substring(0, spaceIndex);
    const rest = line.substring(spaceIndex + 1);
    
    // Split measurement and tags
    const [measurement, ...tagPairs] = measurementAndTags.split(',');
    
    // Parse tags
    const tags = {};
    tagPairs.forEach(pair => {
      const [key, value] = pair.split('=');
      if (key && value) {
        // Remove quotes if present
        tags[key] = value.replace(/^"(.*)"$/, '$1');
      }
    });

    // Find last space which separates fields from timestamp
    const lastSpaceIndex = rest.lastIndexOf(' ');
    if (lastSpaceIndex === -1) throw new Error('Missing timestamp');
    
    // Extract fields and timestamp
    const fieldsStr = rest.substring(0, lastSpaceIndex);
    const timestamp = rest.substring(lastSpaceIndex + 1);

    // Parse fields
    const fields = {};
    const fieldPairs = fieldsStr.split(' ').filter(Boolean);
    fieldPairs.forEach(pair => {
      const [key, value] = pair.split('=');
      if (key && value) {
        // Handle quoted strings
        if (value.startsWith('"') && value.endsWith('"')) {
          fields[key] = value.slice(1, -1);
        } else if (!isNaN(value)) {
          fields[key] = Number(value);
        } else {
          fields[key] = value;
        }
      }
    });

    return {
      measurement,
      tags,
      fields,
      // Convert nanosecond timestamp to Date
      timestamp: new Date(Math.floor(parseInt(timestamp) / 1000000))
    };
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