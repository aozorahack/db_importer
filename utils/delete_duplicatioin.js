require('dotenv').config();

const mongodb = require('mongodb');
const mongodb_credential = process.env.AOZORA_MONGODB_CREDENTIAL || '';
const mongodb_host = process.env.AOZORA_MONGODB_HOST || 'localhost';
const mongodb_port = process.env.AOZORA_MONGODB_PORT || '27017';
const mongo_url = `mongodb://${mongodb_credential}${mongodb_host}:${mongodb_port}/aozora`;

const delete_duplicates = async (collection, id_condition) => {
  const bulk = collection.initializeUnorderedBulkOp();
  const cursor = collection.aggregate([
    { $group: {
      _id: id_condition,
      dups: { $addToSet: '$_id'},
      count: { '$sum': 1}
    }},
    { $match: {
      count: { $gt: 1}
    }}
  ]);

  while(true) {
    const doc = await cursor.next();
    if (doc == null) {
      break;
    }
    doc.dups.shift();
    for (let did of doc.dups) {
      bulk.find({_id: did}).remove();
    }
  }
  // eslint-disable-next-line no-console
  console.log(`${collection.collectionName}: ${bulk.length} entries will be removed`);
  if (bulk.length > 0) {
    let result = await bulk.execute();
    console.log('done\n', result); // eslint-disable-line no-console
  }
};

const run = async () => {
  const db = await mongodb.MongoClient.connect(mongo_url);
  try {
    await Promise.all([delete_duplicates(db.collection('books'), {book_id: '$book_id'}),
                       delete_duplicates(db.collection('persons'), {person_id: '$person_id'})]);
  } catch (error) {
    console.error(error);
  }
  db.close();
};

run();
