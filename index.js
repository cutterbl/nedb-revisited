import DataStore from './dist/nedb-revisited.js';

const init = async function() {
  const list = await fetch('./movies.json').then(response => response.json());
  let db;
  new DataStore({
    filename: '/Users/stephen.blades/Projects/nedb-revisited/foo.db',
    autoload: true
  })
    .then(ds => {
      db = ds;
      return db.insert(list);
    })
    .then(() => db.ensureIndex({ fieldName: 'id' }))
    .then(() => db.ensureIndex({ fieldName: 'first_name' }))
    .then(() => db.ensureIndex({ fieldName: 'last_name' }))
    .then(() => db.ensureIndex({ fieldName: 'title' }))
    .then(
      () =>
        db.find(
          {
            $or: [
              { first_name: { $regex: /ou/i } },
              { last_name: { $regex: /ou/i } },
              { title: { $regex: /ou/i } }
            ]
          },
          {},
          true
        )
      /*db
        .getAll()
        .sort({ title: 1 })
        .exec()*/
    )
    .then(docs => {
      console.log(docs);
    });
};

init();
