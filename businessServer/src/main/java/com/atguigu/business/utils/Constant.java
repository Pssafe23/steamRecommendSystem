package com.atguigu.business.utils;

public class Constant {

    //************** FOR MONGODB ****************

    public static String MONGODB_DATABASE = "gamerecommender";

    public static String MONGODB_USER_COLLECTION= "User";

    public static String MONGODB_MOVIE_COLLECTION = "Game";

    public static String MONGODB_RATING_COLLECTION = "Rating";

    public static String MONGODB_TAG_COLLECTION = "Tag";

    public static String MONGODB_AVERAGE_MOVIES_SCORE_COLLECTION = "Averagegames";

    public static String MONGODB_MOVIE_RECS_COLLECTION = "GameRecs";

    public static String MONGODB_RATE_MORE_MOVIES_COLLECTION = "RateMoregames";

    public static String MONGODB_RATE_MORE_MOVIES_RECENTLY_COLLECTION = "RateMoreRecentlygames";

    public static String MONGODB_STREAM_RECS_COLLECTION = "StreamRecs";

    public static String MONGODB_USER_RECS_COLLECTION = "UserRecs";

    public static String MONGODB_GENRES_TOP_MOVIES_COLLECTION = "GenresTopgames";

    //************** FOR ELEASTICSEARCH ****************

    public static String ES_INDEX = "gamerecommender";

    public static String ES_MOVIE_TYPE = "Game";


    //************** FOR MOVIE RATING ******************

    public static String MOVIE_RATING_PREFIX = "MOVIE_RATING_PREFIX";

    public static int REDIS_MOVIE_RATING_QUEUE_SIZE = 40;
}
