var cassandra = require('cassandra-driver');

var client = new cassandra.Client({ contactPoints: [process.env.CASSANDRA_IP || 'cassandra'], localDataCenter: 'datacenter1' });

/*
 * GET home page.
 */

exports.init_cassandra = function (req, res) {
	// console.log(client);
	console.log('Cassandra IP:', process.env.CASSANDRA_IP);
	client.connect()
		.then(function () {
			const query = "CREATE KEYSPACE IF NOT EXISTS people WITH replication =" +
				"{'class': 'SimpleStrategy', 'replication_factor': '1' }";
			return client.execute(query);
		})
		.then(function () {
			const query = "CREATE TABLE IF NOT EXISTS people.subscribers" +
				" (id uuid, name text, address text, email text, phone text, PRIMARY KEY (id))";
			return client.execute(query);
		})
		.then(function () {
			return client.metadata.getTable('people', 'subscribers');
		})
		.then(function (table) {
			console.log('Table information');
			console.log('- Name: %s', table.name);
			console.log('- Columns:', table.columns);
			console.log('- Partition keys:', table.partitionKeys);
			console.log('- Clustering keys:', table.clusteringKeys);
		})
		.then(function () {
			console.log('Read cluster info');
			var str = '{"hosts": [';
			var i = 0;
			client.hosts.forEach(function (host) {
				console.log('host-------------------------', host);
				i++;
				str += '{"address" : "' + host.address + '", "version" : "' + host.cassandraVersion + '", "rack" : "' + host.rack + '", "datacenter" : "' + host.datacenter + '"}';
				console.log("hosts.length: " + client.hosts.length);
				if (i < client.hosts.length)
					str += ',';

			});
			str += ']}';
			console.log('JSON string: ' + str);
			var jsonHosts = JSON.parse(str);
			res.render('cassandra', { page_title: "Cassandra Details", data: jsonHosts.hosts });
			console.log('initCassandra: success');
		})
		.catch(function (err) {
			console.error('There was an error', err);
			res.status(404).send({ msg: err });
			return client.shutdown();
		});

};

exports.init_cassandra_orm = function (req, res) {
	//init cassandra using ORM express-cassandra
	var models = require('express-cassandra');

	//Tell express-cassandra to use the models-directory, and
	//use bind() to load the models using cassandra configurations.
	console.log(__dirname);
	models.setDirectory('D:/Tasks/POC Cassandra/express-cassandra-crud/models').bind(
		{
			clientOptions: {
				contactPoints: ['127.0.0.1'],
				protocolOptions: { port: 9042 },
				keyspace: 'mykeyspace',
				queryOptions: { consistency: models.consistencies.one }
			},
			ormOptions: {
				defaultReplicationStrategy: {
					class: 'SimpleStrategy',
					replication_factor: 1
				},
				migration: 'safe'
			}
		},
		function (err) {
			if (err) throw err;
			var john = new models.instance.Person({
				name: "John",
				surname: "Doe",
				age: 32
			});
			// save data using Promise
			john.saveAsync()
				.then(function () {
					console.log('Yuppiie! John has been added');
				})
				.catch(function (err) {
					console.log(err);
				});

			// Find inserted record	
			models.instance.Person.findOneAsync({ name: 'John' })
				.then(function (john) {
					console.log('Found ' + john.name + ' to be ' + john.age + ' years old!');
				})
				.catch(function (err) {
					console.log(err);
				});

				return res.send('Done, Goto console for results');	
			// You'll now have a `person` table in cassandra created against the model
			// schema you've defined earlier and you can now access the model instance
			// in `models.instance.Person` object containing supported orm operations.

		}
	);
};	
