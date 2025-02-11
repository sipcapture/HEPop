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
    if (!value) return value;
    if (value === 'true') return true;
    if (value === 'false') return false;
    if (value === 'null' || value === 'NULL') return null;
    if (value.startsWith('"')) return value.slice(1, -1);
    const num = value.includes('.') ? parseFloat(value) : parseInt(value);
    return isNaN(num) ? value : num;
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
      // Handle different timestamp formats
      if (/^\d{19}$/.test(timestamp)) {
        // Nanosecond precision - store as nanoseconds and provide milliseconds
        const nanos = BigInt(timestamp);
        result.timestamp = nanos;
        result.timestampMs = Number(nanos / BigInt(1000000));
        if (process.env.DEBUG) {
          console.log('Parsed nanosecond timestamp:', {
            original: timestamp,
            nanos: nanos.toString(),
            ms: result.timestampMs,
            date: new Date(result.timestampMs).toISOString()
          });
        }
      } else if (/^\d+$/.test(timestamp)) {
        // Regular numeric timestamp - assume milliseconds
        result.timestampMs = parseInt(timestamp);
        result.timestamp = BigInt(result.timestampMs) * BigInt(1000000);
        if (process.env.DEBUG) {
          console.log('Parsed millisecond timestamp:', {
            original: timestamp,
            ms: result.timestampMs,
            date: new Date(result.timestampMs).toISOString()
          });
        }
      } else {
        // Fallback to current time
        const now = Date.now();
        result.timestampMs = now;
        result.timestamp = BigInt(now) * BigInt(1000000);
        if (process.env.DEBUG) {
          console.log('Using current timestamp:', {
            ms: result.timestampMs,
            date: new Date(result.timestampMs).toISOString()
          });
        }
      }
    } else if (config.addTimestamp) {
      // Current time
      const now = Date.now();
      result.timestampMs = now;
      result.timestamp = BigInt(now) * BigInt(1000000);
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