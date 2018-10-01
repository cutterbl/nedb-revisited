import DataStore from './src/index';
new DataStore({
  filename: '/Users/stephen.blades/Projects/nedb-revisited/foo.db',
  autoload: true
}).then(ds => {
  let db = ds;
  const doc = [
    {
      artist: 'Led Zeppelin',
      title: `I Can't Quit You Babe`,
      album: 'Led Zeppelin I',
      year: 1969,
      genre: ['rock']
    },
    {
      artist: 'Jimi Hendrix',
      title: 'Hey Joe',
      album: 'Are You Experienced',
      year: 1968,
      genre: ['rock', 'blues']
    },
    {
      artist: 'Crowded House',
      title: 'Our House'
    },
    {
      artist: 'Jimi Hendrix',
      title: 'Red House'
    }
  ];
  db.insert(doc).then(() => {
    db.ensureIndex({ fieldName: 'artist' }).then(() => {
      db.ensureIndex({ fieldName: 'title' }).then(() => {
        db.find(
          { $or: [{ artist: { $regex: /ou/i } }, { title: { $regex: /ou/i } }] },
          {},
          true
        ).then(docs => {
          console.log(docs);
        });
      });
    });
  });
});
