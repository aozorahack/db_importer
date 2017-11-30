require('dotenv').config();

const mongodb = require('mongodb');
const mongodb_credential = process.env.AOZORA_MONGODB_CREDENTIAL || '';
const mongodb_host = process.env.AOZORA_MONGODB_HOST || 'localhost';
const mongodb_port = process.env.AOZORA_MONGODB_PORT || '27017';
const mongo_url = `mongodb://${mongodb_credential}${mongodb_host}:${mongodb_port}/aozora`;

const fix_fullname = async (db) => {
  const books = db.collection('books');
  let duplicates = [];

  cursor = books.aggregate([
    { $group: {
      _id: {book_id: "$book_id"},
      dups: { $addToSet: "$_id"},
      count: { "$sum": 1}
    }},
    { $match: {
      count: { $gt: 1}
    }}
  ]);

  while(await cursor.hasNext()) {
    const doc = await cursor.next();
    if (doc == null) {
      return;
    }
    doc.dups.shift();
    duplicates.push(doc.dups[0]);
    // console.log(duplicates);
  }
  console.log(duplicates.length);

  const bulk = books.initializeUnorderedBulkOp();
  // console.log(await books.find({_id: {$in: duplicates}}).count());
  
  bulk.find({_id: {$in: duplicates}}).remove();
  bulk.execute();

  db.close();

/*
  find().each((err, book) => {
    if (book == null) {
      db.close();
      return;
    }

      
    books.update({_id: book._id}, book);
  });
*/
}

const run = async () => {
  const db = await mongodb.MongoClient.connect(mongo_url);
  fix_fullname(db);
};

run();
