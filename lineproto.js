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
    // Debug timestamp parsing
    const debugTimestamp = (ts, source) => {
      if (process.env.DEBUG) {
        console.log(`Parsing timestamp: ${ts} (${typeof ts}) from ${source}`);
      }
    };
  
    // Split line into measurement, tags, fields, and timestamp
    const parts = line.split(' ');
    const lastPart = parts[parts.length - 1];
    let timestamp;
  
    // Handle nanosecond precision timestamps
    if (/^\d{19}$/.test(lastPart)) {
      debugTimestamp(lastPart, 'nanoseconds');
      // Convert nanoseconds to milliseconds for Date
      const nanos = BigInt(lastPart);
      timestamp = Number(nanos / 1000000n); // Convert to milliseconds
    } else if (/^\d+$/.test(lastPart)) {
      debugTimestamp(lastPart, 'numeric');
      // Assume milliseconds if less than 19 digits
      timestamp = parseInt(lastPart);
    } else {
      debugTimestamp(lastPart, 'fallback');
      // Use current time if no valid timestamp
      timestamp = Date.now();
    }
  
    // Extract measurement and tags
    const [measurementAndTags] = parts[0].split(' ');
    const [measurement, ...tagParts] = measurementAndTags.split(',');
  
    // Parse tags
    const tags = {};
    tagParts.forEach(tag => {
      const [key, value] = tag.split('=');
      if (key && value) {
        tags[key] = value.replace(/^"(.*)"$/, '$1'); // Remove quotes
      }
    });
  
    // Parse fields
    const fieldString = parts.slice(1, -1).join(' ');
    const fields = {};
    const fieldParts = fieldString.match(/(\w+)="([^"]*)"|\w+=[^,\s]+/g) || [];
    
    fieldParts.forEach(field => {
      const [key, value] = field.split('=');
      if (key && value !== undefined) {
        // Remove quotes if present
        const cleanValue = value.replace(/^"(.*)"$/, '$1');
        // Convert to number if possible
        fields[key] = isNaN(cleanValue) ? cleanValue : parseFloat(cleanValue);
      }
    });
  
    if (process.env.DEBUG) {
      console.log('Parsed line protocol:', {
        measurement,
        tags,
        fields,
        timestamp: new Date(timestamp).toISOString()
      });
    }
  
    return {
      measurement,
      tags,
      fields,
      timestamp
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