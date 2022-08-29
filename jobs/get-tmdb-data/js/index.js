const axios = require("axios").default;
const ObjectsToCsv = require('objects-to-csv');
const fs = require("fs");
let filename = "collection-2-a.json"
let personRawData = fs.readFileSync(`../../tmdbdata/persons-split/collection-2/${filename}`);
let personData = JSON.parse(personRawData);
const [, , API_KEY] = process.argv;
const retries = 5;
const axiosTimeout = 30000; //milliseconds

const personNormalizer = (data) => {
  var nowDate = new Date().toISOString().slice(0, 19).replace("T", " ");
  let ActorData = {
    actor_id: data.id,
    name: data.name,
    description: data.biography,
    dob: data.birthday,
    dod: data.deathday,
    gender: data.gender,
    main_img: `https://image.tmdb.org/t/p/original${data.profile_path}`,
    created_at: nowDate,
    updated_at: nowDate
  };
  return ActorData;
}

const getActorsDataFromTmdb = async () => {
  console.log("Filename::", filename);
  var actorsData = [];
  var csvTime = new Date().getTime();
  let startTime = new Date().toTimeString().slice(0, 8);
  console.log("StartTime::", startTime);
  for(let person of personData) {
    let actor_id = person.id;
    try {
      let response = await axios.get(`https://api.themoviedb.org/3/person/${actor_id}?api_key=${API_KEY}`, {timeout: axiosTimeout, validateStatus: false});
      if(response.status == 200) {
        let normalizedData = personNormalizer(response.data);
        actorsData.push(normalizedData);
      } else {
        console.log("Cannot fetch actor data:",actor_id);
      }
    } catch(err) {
      console.log(err);
      const csv = new ObjectsToCsv(actorsData);
      actorsData = [];
      await csv.toDisk(`../../tmdbdata/actors-data(${csvTime}).csv`, { append: true });
      setTimeout(()=> { console.log("Waiting for sometime because of some error");}, 10000);
      continue;
    }
  }
  let endTime = new Date().toTimeString().slice(0, 8);
  console.log("EndTime::", endTime);
  const csv = new ObjectsToCsv(actorsData);
  await csv.toDisk(`../../tmdbdata/actors-data(${csvTime}).csv`, { append: true });
  return true;
};

getActorsDataFromTmdb();
