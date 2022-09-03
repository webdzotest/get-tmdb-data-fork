const axios = require("axios").default;
const ObjectsToCsv = require('objects-to-csv');
const fs = require("fs");
// let filename = "collection-5-d.json"
let moviesRawData = fs.readFileSync(`../../tmdbdata/movies-some.json`);
let tmdbMoviesData = JSON.parse(moviesRawData);
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

// Movies 

const normalizeMovie = (data) => {
  let movieData = {
    ref_id: data.id,
    adult_movie: data.adult,
    main_img: `https://image.tmdb.org/t/p/original/${data.poster_path}`,
    bg_img: `https://image.tmdb.org/t/p/original/${data.backdrop_path}`,
    overview: data.overview,
    title: data.original_title,
    budget: data.budget,
    revenue: data.revenue,
    duration: data.runtime,
    release_date: data.release_date,
  }

  return movieData;
};

const normalizeMovieData = (data, maxRefId) => {
  let translationsData = [];
  let imagesData = [];
  let MovieActorData = [];
  let ActorRoleData = [];
  let ActorCrewData = [];
  let videosData = [];
  let keywordsData = [];
  // Translations Data
  data.translations.translations.map(item=> {
    translationsData.push({
      movieId: maxRefId,
      name: item.name,
      identifier: item.iso_639_1,
      display_name: item.english_name,
      overview: item.data.overview,
      title: item.data.title
    })
  });

  // Cast Data
  data.credits.cast.map(item=> {
    ActorRoleData.push({
      name: item.name,
      character: item.character,
      year: data.release_date != "" ? parseInt(data.release_date.slice(0, 4)) : null,
      movie_tv_id: maxRefId,
      movie_tv_name: data.original_title,
      type: 'movie',
      department: item.known_for_department,
      order: item.order,
      actor_id: item.id
    });

    MovieActorData.push({
      movie_id: maxRefId,
      actor_id: item.id,
      cast_name: item.original_name,
      role_priority: item.order
    });
  });

  // Crew Data
  data.credits.crew.map(item=> {
    ActorCrewData.push({
      name: item.original_name,
      year: data.release_date != "" ? parseInt(data.release_date.slice(0, 4)) : null,
      movie_tv_id: maxRefId,
      movie_tv_name: data.original_title,
      type: 'movie',
      department: item.department,
      job: item.job,
      actor_id: item.id
    })
  });

  // Images Data
  data.images.backdrops.map(item=> {
    imagesData.push({
      url: `https://image.tmdb.org/t/p/original/${item.file_path}`,
      type: 'movie',
      movie_tv_id: maxRefId,
      img_type: 'backdrop'
    })
  })

  data.images.logos.map(item=> {
    imagesData.push({
      url: `https://image.tmdb.org/t/p/w500/${item.file_path}`,
      type: 'movie',
      movie_tv_id: maxRefId,
      img_type: 'logo'
    })
  });

  data.images.posters.map(item=> {
    imagesData.push({
      url: `https://image.tmdb.org/t/p/w500/${item.file_path}`,
      type: 'movie',
      movie_tv_id: maxRefId,
      img_type: 'poster'
    })
  });

  data.videos.results.map(item=> {
    videosData.push({
      language_code: item.iso_639_1,
      title: item.name,
      movie_tv_id: maxRefId,
      media_type: 'movie',
      source: item.site,
      source_url: item.site == 'YouTube' ? `https://www.youtube.com/watch?v=${item.key}` : '',
      created_at: item.published_at,
      type: item.type
    })
  })

  data.keywords.keywords.map(item=> {
    keywordsData.push({
      name: item.name.toLowerCase(),
      media_type: 'movie',
      movie_tv_id: maxRefId
    })
  })


  return { translationsData, ActorRoleData, MovieActorData, ActorCrewData, imagesData, videosData, keywordsData };
}

const getMoviesFromTmdb = async () => {
  var movieIncr = 1;
  var error_movies = [];
  var moviesData = [];
  var movieTranslationsData = [];
  var actorRolesData = [];
  var movieActorsData = [];
  var actorCrewsData = [];
  var imagesData = [];
  var videosData = [];
  var keywordsData = [];
  let startTime = new Date().toTimeString().slice(0, 8);
  console.log("StartTime::", startTime);
  const append_to_response = "append_to_response=credits,images,keywords,translations,videos";
    // TMDB API's throws an error ECONNRESET sometimes. So we are retrying it for sometimes.
  for(let movie of tmdbMoviesData) {
    let movie_id = movie.id;
    try{
      let response = await axios.get(`https://api.themoviedb.org/3/movie/${movie_id}?api_key=${API_KEY}&${append_to_response}`, {timeout: axiosTimeout, validateStatus: false});
      if(response.status == 200) {
        let data = response.data;
        let movieData = normalizeMovie(data);
        let { translationsData, ActorRoleData, MovieActorData, ActorCrewData, imageData, videoData, keywordData } = normalizeMovieData(data, movieIncr);
        moviesData.push(movieData);
        movieTranslationsData.push(translationsData);
        actorRolesData.push(ActorRoleData);
        movieActorsData.push(MovieActorData);
        actorCrewsData.push(ActorCrewData);
        imagesData.push(imageData);
        videosData.push(videoData);
        keywordsData.push(keywordData);
        movieIncr+=1;
      } else {
        console.log("Cannot fetch movie data:",movie_id);
      }
    } catch(err) {
      console.log("Error on Movie::", movie_id);
      error_movies.push(movie_id);
      await appendToCsv(moviesData, movieTranslationsData, actorRolesData, movieActorsData, actorCrewsData, imagesData, videosData, keywordsData);
      moviesData = [], movieTranslationsData = [], actorRolesData = [], movieActorsData = [], actorCrewsData = [], imagesData = [], videosData = [], keywordsData = [];
      setTimeout(()=> {console.log("ERRCONNSET happened. Waiting for 3s...")}, 3000);
      continue;
    }
  }
  let endTime = new Date().toTimeString().slice(0, 8);
  console.log("EndTime::", endTime);
  await appendToCsv(moviesData, movieTranslationsData, actorRolesData, movieActorsData, actorCrewsData, imagesData, videosData, keywordsData);
  console.log("Errored Movies");
  console.log(error_movies);
  return true;
}

const appendToCsv = async (moviesData, movieTranslationsData, actorRolesData, movieActorsData, actorCrewsData, imagesData, videosData, keywordsData) => {
  const moviesCsvData = new ObjectsToCsv(moviesData);
  await moviesCsvData.toDisk(`../../tmdbdata/${csvTime}/movies-data.csv`, { append: true });
  const translationsCsvData = new ObjectsToCsv(movieTranslationsData);
  await translationsCsvData.toDisk(`../../tmdbdata/${csvTime}/translations-data.csv`, { append: true });
  const actorRoleCsvData = new ObjectsToCsv(actorRolesData);
  await actorRoleCsvData.toDisk(`../../tmdbdata/${csvTime}/actor-roles-data.csv`, { append: true });
  const movieActorsCsvData = new ObjectsToCsv(movieActorsData);
  await movieActorsCsvData.toDisk(`../../tmdbdata/${csvTime}/movie-actors-data.csv`, { append: true });
  const actorCrewsCsvData = new ObjectsToCsv(actorCrewsData);
  await actorCrewsCsvData.toDisk(`../../tmdbdata/${csvTime}/actor-crews-data.csv`, { append: true });
  const imagesCsvData = new ObjectsToCsv(imagesData);
  await imagesCsvData.toDisk(`../../tmdbdata/${csvTime}/images-data.csv`, { append: true });
  const videosCsvData = new ObjectsToCsv(videosData);
  await videosCsvData.toDisk(`../../tmdbdata/${csvTime}/videos-data.csv`, { append: true });
  const keywordsCsvData = new ObjectsToCsv(keywordsData);
  await keywordsCsvData.toDisk(`../../tmdbdata/${csvTime}/keywords-data.csv`, { append: true });
}

getMoviesFromTmdb();

// getActorsDataFromTmdb();
