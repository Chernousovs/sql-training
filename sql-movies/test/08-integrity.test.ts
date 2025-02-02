import _ from "lodash";
import { Database } from "../src/database";
import {
  selectGenreById,
  selectDirectorById,
  selectActorById,
  selectKeywordById,
  selectProductionCompanyById,
  selectMovieById
} from "../src/queries/select";
import { ACTORS, DIRECTORS, GENRES, KEYWORDS, MOVIES, MOVIE_ACTORS, MOVIE_DIRECTORS, MOVIE_GENRES, MOVIE_KEYWORDS, MOVIE_PRODUCTION_COMPANIES, PRODUCTION_COMPANIES } from "../src/table-names";
import { minutes } from "./utils";

describe("Foreign Keys", () => {
  let db: Database;

  beforeAll(async () => {
    db = await Database.fromExisting("06", "07");
    await db.execute("PRAGMA foreign_keys = ON");
  }, minutes(3));

  it(
    "should not be able delete genres if any movie is linked",
    async done => {
      const genreId = 5;
      const query = 
      `DELETE
        FROM ${GENRES}
      WHERE id = ${genreId} 
        AND id NOT IN(SELECT ${MOVIE_GENRES}.GENRE_ID
                        FROM ${MOVIE_GENRES})`;
      try {
        await db.delete(query);
      } catch (e) {}

      const row = await db.selectSingleRow(selectGenreById(genreId));
      expect(row).toBeDefined();

      done();
    },
    minutes(10)
  );

  it(
    "should not be able delete director if any movie is linked",
    async done => {
      const directorId = 7;
      const query = 
      `DELETE
        FROM ${DIRECTORS}
      WHERE id = ${directorId} 
        AND id NOT IN(SELECT ${MOVIE_DIRECTORS}.director_id
                        FROM ${MOVIE_DIRECTORS})`;
      try {
        await db.delete(query);
      } catch (e) {}

      const row = await db.selectSingleRow(selectDirectorById(directorId));
      expect(row).toBeDefined();

      done();
    },
    minutes(10)
  );

  it(
    "should not be able delete actor if any movie is linked",
    async done => {
      const actorId = 10;
      const query = 
      `DELETE 
        FROM ${ACTORS}
      WHERE id = ${actorId} 
        AND id NOT IN(SELECT ${MOVIE_ACTORS}.actor_id
                        FROM ${MOVIE_ACTORS})`;
      try {
        await db.delete(query);
      } catch (e) {}

      const row = await db.selectSingleRow(selectActorById(actorId));
      expect(row).toBeDefined();

      done();
    },
    minutes(10)
  );

  it(
    "should not be able delete keyword if any movie is linked",
    async done => {
      const keywordId = 12;
      const query = 
      `DELETE 
        FROM ${KEYWORDS}
      WHERE id = ${keywordId} 
        AND id NOT IN(SELECT ${MOVIE_KEYWORDS}.keyword_id
                        FROM ${MOVIE_KEYWORDS})`;
      try {
        await db.delete(query);
      } catch (e) {}

      const row = await db.selectSingleRow(selectKeywordById(keywordId));
      expect(row).toBeDefined();

      done();
    },
    minutes(10)
  );

  it(
    "should not be able delete production company if any movie is linked",
    async done => {
      const companyId = 12;
      const query = 
      `DELETE 
        FROM ${PRODUCTION_COMPANIES}
      WHERE id = ${companyId} 
        AND id NOT IN(SELECT ${MOVIE_PRODUCTION_COMPANIES}.company_id
                        FROM ${MOVIE_PRODUCTION_COMPANIES})`;
      try {
        await db.delete(query);
      } catch (e) {}

      const row = await db.selectSingleRow(
        selectProductionCompanyById(companyId)
      );
      expect(row).toBeDefined();

      done();
    },
    minutes(10)
  );

  it(
    "should not be able delete movie if there are any linked data present",
    async done => {
      const movieId = 100;
      const query = 
      `DELETE
        FROM ${MOVIES}
      WHERE id = ${movieId} 
        AND id NOT IN(SELECT ${MOVIE_GENRES}.movie_id
                        FROM ${MOVIE_GENRES}) 
        AND id NOT IN(SELECT ${MOVIE_ACTORS}.movie_id
                        FROM ${MOVIE_ACTORS}) 
        AND id NOT IN(SELECT ${MOVIE_DIRECTORS}.movie_id
                        FROM ${MOVIE_DIRECTORS}) 
        AND id NOT IN(SELECT ${MOVIE_KEYWORDS}.movie_id
                        FROM ${MOVIE_KEYWORDS}) 
        AND id NOT IN(SELECT ${MOVIE_PRODUCTION_COMPANIES}.movie_id
                        FROM ${MOVIE_PRODUCTION_COMPANIES})`;
      try {
        await db.delete(query);
      } catch (e) {}
      
      const row = await db.selectSingleRow(selectMovieById(movieId));
      expect(row).toBeDefined();

      done();
    },
    minutes(10)
  );

  it(
    "should be able to delete movie",
    async done => {
      const movieId = 5915;
      const query = 
      `DELETE FROM ${MOVIE_GENRES} WHERE movie_id = ${movieId};
      DELETE FROM ${MOVIE_ACTORS} WHERE movie_id = ${movieId};
      DELETE FROM ${MOVIE_DIRECTORS} WHERE movie_id = ${movieId};
      DELETE FROM ${MOVIE_KEYWORDS} WHERE movie_id = ${movieId};
      DELETE FROM ${MOVIE_PRODUCTION_COMPANIES} WHERE movie_id = ${movieId};
      DELETE FROM ${MOVIES} WHERE id = ${movieId};`;

      await db.delete(query);

      const row = await db.selectSingleRow(selectMovieById(movieId));
      expect(row).toBeUndefined();

      done();
    },
    minutes(10)
  );
});
