package com.camillepradel.movierecommender.model.db;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionWork;
import org.neo4j.driver.Value;

import static org.neo4j.driver.Values.parameters;


public class Neo4jDatabase extends AbstractDatabase implements AutoCloseable {
    
    private final Driver driver;
    
    public Neo4jDatabase(String uri, String user, String password) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }
    
    @Override
    public List<Movie> getAllMovies() {
        // TODO: write query to retrieve all movies from DB
        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                List<Movie> movies = new LinkedList<>();
                Result result = tx.run( "MATCH (m:Movie)-[:CATEGORIZED_AS]->(g:Genre) WITH m, g ORDER BY m.id RETURN m,collect(g)" );
                while ( result.hasNext() ) {
                    Record row = result.next();
                    int movieId = row.get(0).get("id").asInt();
                    String movieTitle = row.get(0).get("title").asString();
                    Iterable<Value> genres = row.get(1).values();
                    List<Genre> movieGenres = new LinkedList<>();
                    genres.forEach(genre->{movieGenres.add(new Genre(genre.get("id").asInt(),genre.get("name").asString()));});
                    movies.add(new Movie(movieId, movieTitle, movieGenres));
                }
                return movies;
            });
        }
    }

    @Override
    public List<Movie> getMoviesRatedByUser(int userId) {
        // TODO: write query to retrieve all movies rated by user with id userId
        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                List<Movie> movies = new LinkedList<>();
                Result result = tx.run(
                        "MATCH (u:User{id:" + Integer.toString(userId) + "})"
                                + "-[:RATED]->(m:Movie)"
                                + "-[:CATEGORIZED_AS]->(g:Genre) RETURN m, collect(g)");
                while ( result.hasNext() ) {
                    Record row = result.next();
                    int movieId = row.get(0).get("id").asInt();
                    String movieTitle = row.get(0).get("title").asString();
                    Iterable<Value> genres = row.get(1).values();
                    List<Genre> movieGenres = new LinkedList<>();
                    genres.forEach(genre->{movieGenres.add(new Genre(genre.get("id").asInt(),genre.get("name").asString()));});
                    movies.add(new Movie(movieId, movieTitle, movieGenres));
                }
                return movies;
            });
        }
    }

    @Override
    public List<Rating> getRatingsFromUser(int userId) {
        // TODO: write query to retrieve all ratings from user with id userId
        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                List<Rating> ratings = new LinkedList<>();
                Result result = tx.run(
                        "MATCH (u:User{id:" + Integer.toString(userId) + "})"
                                + "-[r:RATED]->(m:Movie)"
                                + "-[:CATEGORIZED_AS]->(g:Genre) RETURN u, r, m, collect(g)");
                while ( result.hasNext() ) {
                    Record row = result.next();
                    // Information from user
                    int uId = row.get(0).get("id").asInt();
                    // Information from rated
                    int note = row.get(1).get("note").asInt();
                    // Information from movie
                    int mId = row.get(2).get("id").asInt();
                    String movieTitle = row.get(2).get("title").asString();
                    // Information from genre
                    Iterable<Value> genres = row.get(3).values();
                    List<Genre> movieGenres = new LinkedList<>();
                    genres.forEach(
                            genre -> {
                                movieGenres.add(new Genre(genre.get("id").asInt(),genre.get("name").asString()));
                            }
                    );
                    // Create Movie
                    Movie movie = new Movie(mId, movieTitle, movieGenres);
                    // Create ratings
                    ratings.add(
                        new Rating(
                            movie,
                            uId,
                            note
                        )
                    );
                }
                return ratings;
            });
        }
    }

    @Override
    public void addOrUpdateRating(Rating rating) {
        // EXAMPLE TRUE : RETURN EXISTS( (:User{id:8})-[:RATED]->(:Movie{id:7}) )
        // EXAMPLE FALSE : RETURN EXISTS( (:User{id:8})-[:RATED]->(:Movie{id:1}) )
        // TODO: add query which
        //         - add rating between specified user and movie if it doesn't exist
        //         - update it if it does exist
    }

    @Override
    public List<Rating> processRecommendationsForUser(int userId, int processingMode) {
        // TODO: process recommendations for specified user exploiting other users ratings
        //       use different methods depending on processingMode parameter
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        Genre genre2 = new Genre(2, "genre2");
        List<Rating> recommendations = new LinkedList<Rating>();
        String titlePrefix;
        if (processingMode == 0) {
            titlePrefix = "0_";
        } else if (processingMode == 1) {
            titlePrefix = "1_";
        } else if (processingMode == 2) {
            titlePrefix = "2_";
        } else {
            titlePrefix = "default_";
        }
        recommendations.add(new Rating(new Movie(0, titlePrefix + "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 5));
        recommendations.add(new Rating(new Movie(1, titlePrefix + "Titre 1", Arrays.asList(new Genre[]{genre0, genre2})), userId, 5));
        recommendations.add(new Rating(new Movie(2, titlePrefix + "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));
        recommendations.add(new Rating(new Movie(3, titlePrefix + "Titre 3", Arrays.asList(new Genre[]{genre0, genre1, genre2})), userId, 3));
        return recommendations;
    }

    @Override
    public void close() throws Exception {
        driver.close();
    }
}
