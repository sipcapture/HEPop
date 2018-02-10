/* HEPop DB Connector */

const knex = require('knex')

const connect = function(config){
  return connection = knex({
   client: 'mysql',
   connection: {
     host: config.MYSQL_HOST || 'localhost',
     port: config.MYSQL_PORT || 3306,
     user: config.MYSQL_USER || 'root',
     password: config.MYSQL_PASSWORD || '',
     database: config.MYSQL_DB || 'partition_test',
     multipleStatements: true
   },
   pool: {
     min: 1,
     max: 10
   }
  })  
};

module.exports = connect;
