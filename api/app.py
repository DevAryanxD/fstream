from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_caching import Cache
from tmdbv3api import TMDb, Movie, TV, Discover, Search, Person
from functools import wraps
import requests
import logging
import re
import redis
import time
import os

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Initialize Redis client
redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=int(os.getenv('REDIS_DB', 0)),
    decode_responses=True
)

# Initialize Flask-Caching
cache = Cache(app, config={
    'CACHE_TYPE': 'redis',
    'CACHE_REDIS_HOST': os.getenv('REDIS_HOST', 'localhost'),
    'CACHE_REDIS_PORT': int(os.getenv('REDIS_PORT', 6379)),
    'CACHE_REDIS_DB': int(os.getenv('REDIS_DB', 0)),
    'CACHE_KEY_PREFIX': '',
    'CACHE_DEFAULT_TIMEOUT': 3600
})

# Initialize TMDb
tmdb = TMDb()
tmdb.api_key = os.getenv('TMDB_API_KEY', 'a813c7e080e0c3ee5d3f70a7ed111e53')
tmdb.language = "en-US"
movie_api = Movie()
tv_api = TV()
discover_api = Discover()
search_api = Search()
person_api = Person()
TMDB_BASE_URL = "https://api.themoviedb.org/3"
MAX_TMDB_PAGES = 500
MAX_TMDB_RESULTS = 10000

# Cache genre lists
GENRE_CACHE = {"movie": {}, "tv": {}}
def load_genre_cache():
    try:
        movie_genres = requests.get(f"{TMDB_BASE_URL}/genre/movie/list?api_key={tmdb.api_key}&language={tmdb.language}").json().get("genres", [])
        tv_genres = requests.get(f"{TMDB_BASE_URL}/genre/tv/list?api_key={tmdb.api_key}&language={tmdb.language}").json().get("genres", [])
        GENRE_CACHE["movie"] = {str(g["id"]): g["name"] for g in movie_genres}
        GENRE_CACHE["tv"] = {str(g["id"]): g["name"] for g in tv_genres}
    except Exception as e:
        logger.error(f"Error loading genre cache: {str(e)}")
load_genre_cache()

def list_to_str(items, key="name"):
    if not items:
        return "N/A"
    items_list = list(items)[:10]
    return ", ".join(str(item.get(key, "N/A")) for item in items_list if item.get(key))

def format_media(item, media_type, include_credits=False):
    try:
        is_dict = isinstance(item, dict)
        item_id = item['id'] if is_dict else item.id
        if is_dict and not all(key in item for key in ['genres', 'production_countries', 'spoken_languages', 'runtime']):
            details = movie_api.details(item_id) if media_type == "movie" else tv_api.details(item_id)
        else:
            details = item if is_dict else (movie_api.details(item_id) if media_type == "movie" else tv_api.details(item_id))
        credits = (movie_api.credits(item_id) if media_type == "movie" else tv_api.credits(item_id)) if include_credits else None
        director = next((crew["name"] for crew in credits.crew if crew["job"] == "Director"), None) if credits else None
        if director is None and media_type == "tv":
            director = list_to_str(details.created_by) if hasattr(details, "created_by") and details.created_by else "N/A"
        cast = list_to_str(credits.cast) if credits else "N/A"
        
        videos_url = f"{TMDB_BASE_URL}/{media_type}/{item_id}/videos?api_key={tmdb.api_key}&language={tmdb.language}"
        videos_response = requests.get(videos_url)
        trailer = "N/A"
        if videos_response.status_code == 200:
            videos = videos_response.json().get("results", [])
            trailer_data = next((v for v in videos if v["type"] == "Trailer" and v["site"] == "YouTube"), None)
            if trailer_data:
                trailer = f"https://www.youtube.com/watch?v={trailer_data['key']}"

        ratings_url = f"{TMDB_BASE_URL}/{media_type}/{item_id}/release_dates?api_key={tmdb.api_key}" if media_type == "movie" else f"{TMDB_BASE_URL}/{media_type}/{item_id}/content_ratings?api_key={tmdb.api_key}"
        ratings_response = requests.get(ratings_url)
        content_rating = "N/A"
        if ratings_response.status_code == 200:
            ratings = ratings_response.json().get("results", [])
            if media_type == "movie":
                us_rating = next((r["release_dates"][0]["certification"] for r in ratings if r.get("iso_3166_1") == "US" and r["release_dates"] and r["release_dates"][0]["certification"]), "N/A")
            else:
                us_rating = next((r["rating"] for r in ratings if r.get("iso_3166_1") == "US"), "N/A")
            content_rating = us_rating if us_rating else "N/A"

        title = details.get('title') if is_dict else getattr(details, "title", None)
        name = details.get('name') if is_dict else getattr(details, "name", None)
        poster_path = details.get('poster_path') if is_dict else getattr(details, "poster_path", None)
        release_date = details.get('release_date') if is_dict else getattr(details, "release_date", None)
        first_air_date = details.get('first_air_date') if is_dict else getattr(details, "first_air_date", None)
        vote_average = details.get('vote_average', "N/A") if is_dict else getattr(details, "vote_average", "N/A")
        genres = details.get('genres', []) if is_dict else getattr(details, "genres", [])
        runtime = details.get('runtime', None) if is_dict else getattr(details, "runtime", None)
        spoken_languages = details.get('spoken_languages', []) if is_dict else getattr(details, "spoken_languages", [])
        production_countries = details.get('production_countries', []) if is_dict else getattr(details, "production_countries", [])
        production_companies = details.get('production_companies', []) if is_dict else getattr(details, "production_companies", [])
        networks = details.get('networks', []) if is_dict else getattr(details, "networks", [])
        overview = details.get('overview', "N/A") if is_dict else getattr(details, "overview", "N/A")
        imdb_id = details.get('imdb_id', "") if is_dict else getattr(details, "imdb_id", "")
        number_of_seasons = details.get('number_of_seasons', 0) if is_dict else getattr(details, "number_of_seasons", 0)
        number_of_episodes = details.get('number_of_episodes', 0) if is_dict else getattr(details, "number_of_episodes", 0)
        belongs_to_collection = details.get('belongs_to_collection') if is_dict else getattr(details, "belongs_to_collection", None)
        vote_count = details.get('vote_count', 0) if is_dict else getattr(details, "vote_count", 0)
        popularity = details.get('popularity', 0.0) if is_dict else getattr(details, "popularity", 0.0)
        backdrop_path = details.get('backdrop_path') if is_dict else getattr(details, "backdrop_path", None)
        status = details.get('status', "N/A") if is_dict else getattr(details, "status", "N/A")
        tagline = details.get('tagline', "N/A") if is_dict else getattr(details, "tagline", "N/A")
        budget = details.get('budget', 0) if is_dict else getattr(details, "budget", 0)
        revenue = details.get('revenue', 0) if is_dict else getattr(details, "revenue", 0)

        result = {
            "title": title or name or "N/A",
            "poster": f"https://image.tmdb.org/t/p/original{poster_path}" if poster_path else "",
            "backdrop": f"https://image.tmdb.org/t/p/w780{backdrop_path}" if backdrop_path else "",
            "year": release_date.split("-")[0] if release_date else (first_air_date.split("-")[0] if first_air_date else "N/A"),
            "rating": str(vote_average),
            "vote_count": vote_count,
            "popularity": popularity,
            "content_rating": content_rating,
            "genres": list_to_str(genres),
            "runtime": f"{runtime} min" if runtime else "N/A",
            "director": director,
            "cast": cast,
            "languages": list_to_str(spoken_languages, key="english_name"),
            "countries": list_to_str(production_countries, key="name"),
            "production_companies": list_to_str(production_companies, key="name"),
            "status": status,
            "tagline": tagline,
            "release_date": release_date or first_air_date or "N/A",
            "plot": overview,
            "trailer": trailer,
            "url": f"https://www.themoviedb.org/{media_type}/{item_id}",
            "tmdb_id": str(item_id),
            "imdb_id": imdb_id,
            "media_type": media_type
        }
        if media_type == "movie":
            result["budget"] = budget
            result["revenue"] = revenue
        if media_type == "tv":
            result["networks"] = list_to_str(networks, key="name")
            result["number_of_seasons"] = number_of_seasons
            result["number_of_episodes"] = number_of_episodes
        if media_type == "movie" and belongs_to_collection:
            result["collection"] = {
                "id": str(belongs_to_collection.get("id", "")),
                "name": belongs_to_collection.get("name", "N/A"),
                "poster": f"https://image.tmdb.org/t/p/original{belongs_to_collection.get('poster_path')}" if belongs_to_collection.get('poster_path') else ""
            }
        return result
    except Exception as e:
        logger.error(f"Error formatting {media_type} data: {str(e)}", exc_info=True)
        return None

def format_media_light(item, media_type):
    item_dict = item if isinstance(item, dict) else item.__dict__
    genre_ids = item_dict.get("genre_ids", [])
    genres = [GENRE_CACHE[media_type].get(str(gid), "N/A") for gid in genre_ids if str(gid) in GENRE_CACHE[media_type]]
    result = {
        "title": item_dict.get("title") or item_dict.get("name") or "N/A",
        "poster": f"https://image.tmdb.org/t/p/original{item_dict.get('poster_path')}" if item_dict.get("poster_path") else "",
        "backdrop": f"https://image.tmdb.org/t/p/w780{item_dict.get('backdrop_path')}" if item_dict.get("backdrop_path") else "",
        "year": item_dict.get("release_date", "N/A").split("-")[0] if item_dict.get("release_date") else item_dict.get("first_air_date", "N/A").split("-")[0] if item_dict.get("first_air_date") else "N/A",
        "rating": str(item_dict.get("vote_average", "N/A")),
        "vote_count": item_dict.get("vote_count", 0),
        "popularity": item_dict.get("popularity", 0.0),
        "genres": ", ".join(genres) if genres else "N/A",
        "plot": item_dict.get("overview", "N/A"),
        "release_date": item_dict.get("release_date") or item_dict.get("first_air_date") or "N/A",
        "url": f"https://www.themoviedb.org/{media_type}/{item_dict.get('id')}",
        "tmdb_id": str(item_dict.get("id")),
        "media_type": media_type
    }
    if media_type == "tv":
        result["origin_country"] = ", ".join(item_dict.get("origin_country", [])) if item_dict.get("origin_country") else "N/A"
    return result

def format_person(person):
    try:
        person_dict = person if isinstance(person, dict) else person.__dict__
        return {
            "person_id": str(person_dict.get("id")),
            "name": person_dict.get("name", "N/A"),
            "birthday": person_dict.get("birthday", "N/A"),
            "biography": person_dict.get("biography", "N/A"),
            "profile_path": f"https://image.tmdb.org/t/p/w185{person_dict.get('profile_path')}" if person_dict.get("profile_path") else "",
            "known_for_department": person_dict.get("known_for_department", "N/A")
        }
    except Exception as e:
        logger.error(f"Error formatting person data: {str(e)}", exc_info=True)
        return None

def validate_id(tmdb_id):
    return bool(re.match(r'^\d+$', str(tmdb_id)))

def validate_sort_by(sort_by):
    valid_sort_options = [
        "popularity.desc", "popularity.asc",
        "vote_average.desc", "vote_average.asc",
        "release_date.desc", "release_date.asc"
    ]
    return sort_by if sort_by in valid_sort_options else "popularity.desc"

def record_cache_stats(endpoint, hit):
    try:
        key = f"cache_stats:{endpoint}"
        redis_client.hincrby(key, "hits" if hit else "misses", 1)
        redis_client.hincrby(key, "total", 1)
        logger.debug(f"Recorded cache stats for {endpoint}: hit={hit}")
    except Exception as e:
        logger.error(f"Error recording cache stats for {endpoint}: {str(e)}")

def cached_with_stats(timeout, key_prefix):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            prefix = key_prefix() if callable(key_prefix) else key_prefix
            cache_key = prefix
            endpoint = prefix.replace(f"{request.view_args.get('media_type', '')}_", 
                                   f"{request.view_args.get('media_type', '')}/").replace("_page_", "/page/")
            hit = redis_client.exists(cache_key) > 0
            logger.debug(f"Cache key: {cache_key}, Pre-check Hit: {hit}")
            
            @cache.cached(timeout=timeout, key_prefix=prefix)
            def cached_func(*args, **kwargs):
                logger.debug(f"Cache miss for key: {cache_key}")
                return f(*args, **kwargs)
            
            response = cached_func(*args, **kwargs)
            record_cache_stats(endpoint, hit)
            return response
        return decorated_function
    return decorator

@app.route('/api/cache/stats', methods=['GET'])
def get_cache_stats():
    try:
        stats = {}
        keys = redis_client.keys("cache_stats:*")
        for key in keys:
            endpoint = key.replace("cache_stats:", "")
            data = redis_client.hgetall(key)
            hits = int(data.get("hits", 0))
            misses = int(data.get("misses", 0))
            total = int(data.get("total", 0))
            hit_ratio = (hits / total * 100) if total > 0 else 0
            stats[endpoint] = {
                "hits": hits,
                "misses": misses,
                "total": total,
                "hit_ratio": round(hit_ratio, 2)
            }
        return jsonify({"cache_stats": stats})
    except Exception as e:
        logger.error(f"Error in get_cache_stats: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/person/<person_id>', methods=['GET'])
def get_person_details(person_id):
    if not validate_id(person_id):
        logger.warning(f"Invalid person ID: {person_id}")
        return jsonify({"error": "Invalid person ID"}), 400
    logger.info(f"Fetching person details for TMDb ID: {person_id}")
    try:
        person = person_api.details(person_id)
        if not person:
            logger.warning(f"No person found for TMDb ID: {person_id}")
            return jsonify({"error": "Person not found"}), 404
        formatted = format_person(person)
        if not formatted:
            logger.warning(f"Unable to format person data for TMDb ID: {person_id}")
            return jsonify({"error": "Person not found"}), 404
        return jsonify(formatted)
    except Exception as e:
        logger.error(f"Error in get_person_details: {str(e)}", exc_info=True)
        if "404" in str(e):
            return jsonify({"error": "Person not found"}), 404
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/search', methods=['GET'])
def search_media():
    query = request.args.get('query')
    page = request.args.get('page', 1, type=int)
    if not query:
        logger.warning("No query parameter provided")
        return jsonify({"error": "Query parameter is required"}), 400
    logger.info(f"Received search query: {query}, page: {page}")
    try:
        results = search_api.multi(query, page=page)
        if not results:
            logger.warning(f"No results found for query: {query}")
            return jsonify({"results": [], "page": f"{page} of 1", "total_results": 0, "total_pages": 1}), 200
        formatted_results = [format_media_light(item, item.media_type) for item in results if item.media_type in ["movie", "tv"]]
        total_results = min(results.total_results, MAX_TMDB_RESULTS)
        total_pages = min(results.total_pages, MAX_TMDB_PAGES)
        return jsonify({
            "results": [r for r in formatted_results if r],
            "page": f"{page} of {total_pages}",
            "total_results": total_results,
            "total_pages": total_pages
        })
    except Exception as e:
        logger.error(f"Error in search_media: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/<media_type>/latest', methods=['GET'])
def get_media_latest(media_type):
    if media_type not in ["movie", "tv"]:
        logger.warning(f"Invalid media_type: {media_type}")
        return jsonify({"error": "Invalid media type"}), 400
    logger.info(f"Fetching latest for media_type: {media_type}")
    try:
        url = f"{TMDB_BASE_URL}/{media_type}/latest?api_key={tmdb.api_key}&language={tmdb.language}"
        response = requests.get(url)
        if response.status_code == 429:
            logger.error("TMDb rate limit exceeded")
            return jsonify({"error": "Rate limit exceeded, try again later"}), 429
        if response.status_code == 404:
            logger.warning(f"No latest {media_type} found")
            return jsonify({"results": []}), 200
        if response.status_code != 200:
            logger.warning(f"Failed to fetch latest {media_type}, status: {response.status_code}")
            return jsonify({"error": f"Failed to fetch latest {media_type}"}), 500
        data = response.json()
        formatted = format_media_light(data, media_type)
        return jsonify({"results": [formatted]})
    except Exception as e:
        logger.error(f"Error in get_media_latest: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/collection/<collection_id>', methods=['GET'])
@cached_with_stats(timeout=24*3600, key_prefix=lambda: f"collection_{request.view_args['collection_id']}")
def get_collection(collection_id):
    if not validate_id(collection_id):
        logger.warning(f"Invalid collection ID: {collection_id}")
        return jsonify({"error": "Invalid collection ID"}), 400
    logger.info(f"Fetching collection with ID: {collection_id}")
    try:
        url = f"{TMDB_BASE_URL}/collection/{collection_id}?api_key={tmdb.api_key}&language={tmdb.language}"
        response = requests.get(url)
        if response.status_code == 429:
            logger.error("TMDb rate limit exceeded")
            return jsonify({"error": "Rate limit exceeded, try again later"}), 429
        if response.status_code == 404:
            logger.warning(f"Collection not found for ID: {collection_id}")
            return jsonify({"error": "Collection not found"}), 404
        if response.status_code != 200:
            logger.warning(f"Failed to fetch collection ID: {collection_id}")
            return jsonify({"results": []}), 200
        collection = response.json()
        parts = collection.get("parts", [])
        formatted_parts = []
        for part in parts:
            release_date = part.get("release_date", "").strip()
            genres = [GENRE_CACHE["movie"].get(str(gid), "N/A") for gid in part.get("genre_ids", []) if str(gid) in GENRE_CACHE["movie"]]
            formatted_parts.append({
                "title": part.get("title", "N/A"),
                "poster": f"https://image.tmdb.org/t/p/original{part.get('poster_path')}" if part.get("poster_path") else "",
                "backdrop": f"https://image.tmdb.org/t/p/w780{part.get('backdrop_path')}" if part.get("backdrop_path") else "",
                "year": release_date.split("-")[0] if release_date else "N/A",
                "rating": str(part.get("vote_average", "N/A")),
                "vote_count": part.get("vote_count", 0),
                "popularity": part.get("popularity", 0.0),
                "genres": ", ".join(genres) if genres else "N/A",
                "plot": part.get("overview", "N/A"),
                "release_date": release_date if release_date else "N/A",
                "url": f"https://www.themoviedb.org/movie/{part.get('id')}",
                "tmdb_id": str(part.get("id")),
                "media_type": "movie"
            })
        formatted_parts.sort(key=lambda x: x["release_date"] if x["release_date"] != "N/A" else "1900-01-01")
        return jsonify({
            "tmdb_id": str(collection.get("id")),
            "name": collection.get("name", "N/A"),
            "overview": collection.get("overview", "N/A"),
            "poster": f"https://image.tmdb.org/t/p/original{collection.get('poster_path')}" if collection.get("poster_path") else "",
            "backdrop": f"https://image.tmdb.org/t/p/w780{collection.get('backdrop_path')}" if collection.get("backdrop_path") else "",
            "parts": formatted_parts,
            "total_results": len(formatted_parts)
        })
    except Exception as e:
        logger.error(f"Error in get_collection: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/<media_type>/<tmdb_id>', methods=['GET'])
@cached_with_stats(timeout=2*3600, key_prefix=lambda: f"{request.view_args['media_type']}_{request.view_args['tmdb_id']}")
def get_media_by_id(media_type, tmdb_id):
    if not validate_id(tmdb_id):
        logger.warning(f"Invalid TMDb ID: {tmdb_id}")
        return jsonify({"error": "Invalid TMDb ID"}), 400
    if media_type not in ["movie", "tv"]:
        logger.warning(f"Invalid media_type: {media_type}")
        return jsonify({"error": "Invalid media type"}), 400
    logger.info(f"Received {media_type} TMDb ID: {tmdb_id}")
    try:
        details = movie_api.details(tmdb_id) if media_type == "movie" else tv_api.details(tmdb_id)
        if not details:
            logger.warning(f"No details found for {media_type} TMDb ID: {tmdb_id}")
            return jsonify({"error": f"{media_type.title()} not found"}), 404
        media_data = format_media(details, media_type, include_credits=True)
        if not media_data:
            logger.warning(f"Unable to format {media_type} data for TMDb ID: {tmdb_id}")
            return jsonify({"error": f"{media_type.title()} not found"}), 404
        return jsonify(media_data)
    except Exception as e:
        logger.error(f"Error in get_media_by_id: {str(e)}", exc_info=True)
        if "404" in str(e):
            return jsonify({"error": f"{media_type.title()} not found"}), 404
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/<media_type>/<tmdb_id>/keywords', methods=['GET'])
@cached_with_stats(timeout=24*3600, key_prefix=lambda: f"{request.view_args['media_type']}_{request.view_args['tmdb_id']}_keywords")
def get_media_keywords(media_type, tmdb_id):
    if not validate_id(tmdb_id):
        logger.warning(f"Invalid TMDb ID: {tmdb_id}")
        return jsonify({"error": "Invalid TMDb ID"}), 400
    if media_type not in ["movie", "tv"]:
        logger.warning(f"Invalid media_type: {media_type}")
        return jsonify({"error": "Invalid media type"}), 400
    logger.info(f"Fetching keywords for {media_type} TMDb ID: {tmdb_id}")
    try:
        endpoint = "keywords" if media_type == "movie" else "keywords"
        url = f"{TMDB_BASE_URL}/{media_type}/{tmdb_id}/{endpoint}?api_key={tmdb.api_key}"
        response = requests.get(url)
        if response.status_code == 429:
            logger.error("TMDb rate limit exceeded")
            return jsonify({"error": "Rate limit exceeded, try again later"}), 429
        if response.status_code == 404:
            logger.warning(f"{media_type.title()} not found: {tmdb_id}")
            return jsonify({"error": f"{media_type.title()} not found"}), 404
        if response.status_code != 200:
            logger.warning(f"Failed to fetch keywords for {media_type} ID {tmdb_id}")
            return jsonify({"error": "Failed to fetch keywords"}), 500
        data = response.json()
        raw_keywords = data.get("keywords" if media_type == "movie" else "results", [])
        keywords = [
            {
                "name": kw.get("name", "N/A"),
                "tmdb_id": str(kw.get("id"))
            }
            for kw in raw_keywords if kw.get("id") and kw.get("name")
        ]
        return jsonify({
            "tmdb_id": str(tmdb_id),
            "media_type": media_type,
            "keywords": keywords
        })
    except Exception as e:
        logger.error(f"Error in get_media_keywords: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/<media_type>/<tmdb_id>/credits', methods=['GET'])
@cached_with_stats(timeout=24*3600, key_prefix=lambda: f"{request.view_args['media_type']}_{request.view_args['tmdb_id']}_credits")
def get_media_credits(media_type, tmdb_id):
    if not validate_id(tmdb_id):
        logger.warning(f"Invalid TMDb ID: {tmdb_id}")
        return jsonify({"error": "Invalid TMDb ID"}), 400
    if media_type not in ["movie", "tv"]:
        logger.warning(f"Invalid media_type: {media_type}")
        return jsonify({"error": "Invalid media type"}), 400
    logger.info(f"Fetching credits for {media_type} TMDb ID: {tmdb_id}")
    try:
        if media_type == "movie":
            credits = movie_api.credits(tmdb_id)
            details = movie_api.details(tmdb_id)
        else:
            credits = tv_api.credits(tmdb_id)
            details = tv_api.details(tmdb_id)
        if not credits or not details:
            logger.warning(f"Credits or details not found for {media_type} TMDb ID: {tmdb_id}")
            return jsonify({"error": "Credits not found"}), 404
        title = details.title if media_type == "movie" else details.name
        cast = [
            {
                "name": person.get("name", "N/A"),
                "character": person.get("character", "N/A"),
                "tmdb_id": str(person.get("id")),
                "profile_path": f"https://image.tmdb.org/t/p/w185{person.get('profile_path')}" if person.get("profile_path") else "",
                "known_for_department": person.get("known_for_department", "N/A")
            }
            for person in list(credits.cast)[:10] if person.get("name")
        ]
        directors = [
            {
                "name": person.get("name", "N/A"),
                "tmdb_id": str(person.get("id")),
                "profile_path": f"https://image.tmdb.org/t/p/w185{person.get('profile_path')}" if person.get("profile_path") else "",
                "department": person.get("department", "N/A"),
                "known_for_department": person.get("known_for_department", "N/A")
            }
            for person in credits.crew if person.get("job") == "Director"
        ]
        if not directors and media_type == "tv":
            directors = [
                {
                    "name": person.get("name", "N/A"),
                    "tmdb_id": str(person.get("id")),
                    "profile_path": f"https://image.tmdb.org/t/p/w185{person.get('profile_path')}" if person.get("profile_path") else "",
                    "department": "Creator",
                    "known_for_department": person.get("known_for_department", "N/A")
                }
                for person in getattr(details, "created_by", []) if person.get("name")
            ]
            if not directors:
                logger.warning(f"No directors or creators found for TV TMDb ID: {tmdb_id}")
        return jsonify({
            "tmdb_id": str(tmdb_id),
            "title": title or "N/A",
            "media_type": media_type,
            "cast": cast,
            "directors": directors
        })
    except Exception as e:
        logger.error(f"Error in get_media_credits: {str(e)}", exc_info=True)
        if "404" in str(e):
            return jsonify({"error": f"{media_type.title()} not found"}), 404
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/person/<person_id>/combined_credits', methods=['GET'])
@cached_with_stats(timeout=24*3600, key_prefix=lambda: f"person_{request.view_args['person_id']}_combined_credits")
def get_person_combined_credits(person_id):
    if not validate_id(person_id):
        logger.warning(f"Invalid person ID: {person_id}")
        return jsonify({"error": "Invalid person ID"}), 400
    logger.info(f"Fetching combined credits for person TMDb ID: {person_id}")
    try:
        person_details = requests.get(f"{TMDB_BASE_URL}/person/{person_id}?api_key={tmdb.api_key}&language={tmdb.language}")
        if person_details.status_code == 429:
            logger.error("TMDb rate limit exceeded")
            return jsonify({"error": "Rate limit exceeded, try again later"}), 429
        if person_details.status_code == 404:
            logger.warning(f"No person found for TMDb ID: {person_id}")
            return jsonify({"error": "Person not found"}), 404
        person_data = person_details.json()
        person_name = person_data.get("name", "N/A")
        
        url = f"{TMDB_BASE_URL}/person/{person_id}/combined_credits?api_key={tmdb.api_key}&language={tmdb.language}"
        response = requests.get(url)
        if response.status_code == 429:
            logger.error("TMDb rate limit exceeded")
            return jsonify({"error": "Rate limit exceeded, try again later"}), 429
        if response.status_code == 404:
            logger.warning(f"No credits found for person TMDb ID: {person_id}")
            return jsonify({"error": "Credits not found"}), 404
        if response.status_code != 200:
            return jsonify({"error": "Failed to fetch credits"}), 500
        data = response.json()
        combined = data.get("cast", []) + data.get("crew", [])
        sorted_credits = sorted(combined, key=lambda x: x.get("popularity", 0), reverse=True)
        credits = []
        for item in sorted_credits:
            if item.get("media_type") not in ["movie", "tv"]:
                continue
            role = "cast" if item in data.get("cast", []) else "crew"
            title = item.get("title") or item.get("name") or "N/A"
            poster = f"https://image.tmdb.org/t/p/original{item.get('poster_path')}" if item.get("poster_path") else ""
            credit = {
                "title": title,
                "media_type": item["media_type"],
                "tmdb_id": str(item["id"]),
                "poster": poster,
                "backdrop": f"https://image.tmdb.org/t/p/w780{item.get('backdrop_path')}" if item.get("backdrop_path") else "",
                "role": role,
                "release_date": item.get("release_date") or item.get("first_air_date") or "N/A",
                "year": item.get("release_date", "N/A").split("-")[0] if item.get("release_date") else item.get("first_air_date", "N/A").split("-")[0] if item.get("first_air_date") else "N/A",
                "vote_average": str(item.get("vote_average", "N/A")),
                "vote_count": item.get("vote_count", 0),
                "popularity": item.get("popularity", 0.0)
            }
            if role == "cast":
                credit["character"] = item.get("character", "N/A")
            else:
                credit["job"] = item.get("job", "N/A")
            credits.append(credit)
        return jsonify({
            "person_id": str(person_id),
            "name": person_name,
            "credits": credits
        })
    except Exception as e:
        logger.error(f"Error in get_person_combined_credits: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/tv/<tmdb_id>/seasons', methods=['GET'])
@cached_with_stats(timeout=24*3600, key_prefix=lambda: f"tv_{request.view_args['tmdb_id']}_seasons")
def get_tv_seasons(tmdb_id):
    if not validate_id(tmdb_id):
        logger.warning(f"Invalid TMDb ID: {tmdb_id}")
        return jsonify({"error": "Invalid TMDb ID"}), 400
    logger.info(f"Fetching seasons for TV TMDb ID: {tmdb_id}")
    try:
        details = tv_api.details(tmdb_id)
        if not details:
            logger.warning(f"No TV show found for TMDb ID: {tmdb_id}")
            return jsonify({"error": "TV show not found"}), 404
        seasons = [
            {
                "season_number": season.season_number,
                "name": season.name or f"Season {season.season_number}",
                "episode_count": season.episode_count,
                "air_date": season.air_date or "N/A",
                "poster": f"https://image.tmdb.org/t/p/original{season.poster_path}" if season.poster_path else "",
                "overview": season.overview or "N/A",
                "vote_average": str(season.vote_average) if hasattr(season, "vote_average") else "N/A"
            }
            for season in details.seasons
        ]
        return jsonify({
            "tmdb_id": str(tmdb_id),
            "title": getattr(details, "name", "N/A"),
            "total_seasons": len(seasons),
            "seasons": seasons
        })
    except Exception as e:
        logger.error(f"Error in get_tv_seasons: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/tv/<tmdb_id>/season/<season_number>', methods=['GET'])
@cached_with_stats(timeout=24*3600, key_prefix=lambda: f"tv_{request.view_args['tmdb_id']}_season_{request.view_args['season_number']}")
def get_tv_season_details(tmdb_id, season_number):
    if not validate_id(tmdb_id):
        logger.warning(f"Invalid TMDb ID: {tmdb_id}")
        return jsonify({"error": "Invalid TMDb ID"}), 400
    try:
        season_number = int(season_number)
        if season_number < 0:
            return jsonify({"error": "Season number must be non-negative"}), 400
    except ValueError:
        return jsonify({"error": "Invalid season number"}), 400
    logger.info(f"Fetching season {season_number} for TV TMDb ID: {tmdb_id}")
    try:
        url = f"{TMDB_BASE_URL}/tv/{tmdb_id}/season/{season_number}?api_key={tmdb.api_key}&language={tmdb.language}"
        response = requests.get(url)
        if response.status_code == 429:
            logger.error("TMDb rate limit exceeded")
            return jsonify({"error": "Rate limit exceeded, try again later"}), 429
        if response.status_code == 404:
            logger.warning(f"Season not found for season {season_number} of TV TMDb ID: {tmdb_id}")
            return jsonify({"error": "Season not found"}), 404
        if response.status_code != 200:
            logger.warning(f"Failed to fetch season {season_number} for TV TMDb ID: {tmdb_id}")
            return jsonify({"error": "Failed to fetch season"}), 500
        season_data = response.json()
        episodes = [
            {
                "episode_number": ep.get("episode_number"),
                "name": ep.get("name", "N/A"),
                "air_date": ep.get("air_date", "N/A"),
                "overview": ep.get("overview", "N/A"),
                "poster": f"https://image.tmdb.org/t/p/original{ep.get('still_path')}" if ep.get("still_path") else "",
                "vote_average": str(ep.get("vote_average", "N/A")),
                "runtime": f"{ep.get('runtime')} min" if ep.get("runtime") else "N/A",
                "guest_stars": list_to_str(ep.get("guest_stars", [])) if ep.get("guest_stars") else "N/A"
            }
            for ep in season_data.get("episodes", [])
        ]
        return jsonify({
            "season_number": season_number,
            "season_title": season_data.get("name", f"Season {season_number}"),
            "episodes": episodes,
            "total_episodes": len(episodes)
        })
    except Exception as e:
        logger.error(f"Error in get_tv_season_details: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/<media_type>/popular', methods=['GET'])
@cached_with_stats(timeout=4*3600, key_prefix=lambda: f"{request.view_args['media_type']}_popular_page_{request.args.get('page', '1')}")
def get_media_popular(media_type):
    if media_type not in ["movie", "tv"]:
        logger.warning(f"Invalid media_type: {media_type}")
        return jsonify({"error": "Invalid media type"}), 400
    logger.info(f"Fetching popular {media_type}s, page: {request.args.get('page', 1)}")
    try:
        page = request.args.get('page', 1, type=int)
        results = movie_api.popular(page=page) if media_type == "movie" else tv_api.popular(page=page)
        if not results:
            logger.warning(f"No popular {media_type}s found")
            return jsonify({"results": [], "page": f"{page} of 1", "total_results": 0, "total_pages": 1}), 200
        total_results = min(results.total_results, MAX_TMDB_RESULTS)
        total_pages = min(results.total_pages, MAX_TMDB_PAGES)
        items = [format_media_light(item, media_type) for item in results]
        return jsonify({
            "results": [i for i in items if i],
            "page": f"{page} of {total_pages}",
            "total_results": total_results,
            "total_pages": total_pages
        })
    except Exception as e:
        logger.error(f"Error in get_media_popular: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/<media_type>/top_rated', methods=['GET'])
@cached_with_stats(timeout=4*3600, key_prefix=lambda: f"{request.view_args['media_type']}_top_rated_page_{request.args.get('page', '1')}")
def get_media_top_rated(media_type):
    if media_type not in ["movie", "tv"]:
        logger.warning(f"Invalid media_type: {media_type}")
        return jsonify({"error": "Invalid media type"}), 400
    logger.info(f"Fetching top-rated {media_type}s, page: {request.args.get('page', 1)}")
    try:
        page = request.args.get('page', 1, type=int)
        results = movie_api.top_rated(page=page) if media_type == "movie" else tv_api.top_rated(page=page)
        if not results:
            logger.warning(f"No top-rated {media_type}s found")
            return jsonify({"results": [], "page": f"{page} of 1", "total_results": 0, "total_pages": 1}), 200
        total_results = min(results.total_results, MAX_TMDB_RESULTS)
        total_pages = min(results.total_pages, MAX_TMDB_PAGES)
        items = [format_media_light(item, media_type) for item in results]
        return jsonify({
            "results": [i for i in items if i],
            "page": f"{page} of {total_pages}",
            "total_results": total_results,
            "total_pages": total_pages
        })
    except Exception as e:
        logger.error(f"Error in get_media_top_rated: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/movie/upcoming', methods=['GET'])
@cached_with_stats(timeout=4*3600, key_prefix=lambda: f"movie_upcoming_page_{request.args.get('page', '1')}")
def get_movie_upcoming():
    page = request.args.get('page', 1, type=int)
    logger.info(f"Fetching upcoming movies, page: {page}")
    try:
        results = movie_api.upcoming(page=page)
        if not results:
            logger.warning("No upcoming movies found")
            return jsonify({"results": [], "page": f"{page} of 1", "total_results": 0, "total_pages": 1}), 200
        total_results = min(results.total_results, MAX_TMDB_RESULTS)
        total_pages = min(results.total_pages, MAX_TMDB_PAGES)
        movies = [format_media_light(movie, "movie") for movie in results]
        return jsonify({
            "results": [m for m in movies if m],
            "page": f"{page} of {total_pages}",
            "total_results": total_results,
            "total_pages": total_pages
        })
    except Exception as e:
        logger.error(f"Error in get_movie_upcoming: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/tv/on_the_air', methods=['GET'])
@cached_with_stats(timeout=4*3600, key_prefix=lambda: f"tv_on_the_air_page_{request.args.get('page', '1')}")
def get_tv_on_the_air():
    page = request.args.get('page', 1, type=int)
    logger.info(f"Fetching on the air TV shows, page: {page}")
    try:
        results = tv_api.on_the_air(page=page)
        if not results:
            logger.warning("No on the air TV shows found")
            return jsonify({"results": [], "page": f"{page} of 1", "total_results": 0, "total_pages": 1}), 200
        total_results = min(results.total_results, MAX_TMDB_RESULTS)
        total_pages = min(results.total_pages, MAX_TMDB_PAGES)
        shows = [format_media_light(show, "tv") for show in results]
        return jsonify({
            "results": [s for s in shows if s],
            "page": f"{page} of {total_pages}",
            "total_results": total_results,
            "total_pages": total_pages
        })
    except Exception as e:
        logger.error(f"Error in get_tv_on_the_air: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/<media_type>/trending', methods=['GET'])
@cached_with_stats(timeout=4*3600, key_prefix=lambda: f"{request.view_args['media_type']}_trending_{request.args.get('time_window', 'week')}_page_{request.args.get('page', '1')}")
def get_media_trending(media_type):
    if media_type not in ["movie", "tv"]:
        logger.warning(f"Invalid media_type: {media_type}")
        return jsonify({"error": "Invalid media type"}), 400
    page = request.args.get('page', 1, type=int)
    time_window = request.args.get('time_window', 'week')
    logger.info(f"Fetching trending {media_type}s, page: {page}, time_window: {time_window}")
    try:
        url = f"{TMDB_BASE_URL}/trending/{media_type}/{time_window}?api_key={tmdb.api_key}&language={tmdb.language}&page={page}"
        response = requests.get(url)
        if response.status_code == 429:
            logger.error("TMDb rate limit exceeded")
            return jsonify({"error": "Rate limit exceeded, try again later"}), 429
        if response.status_code != 200:
            logger.warning(f"No trending {media_type}s found")
            return jsonify({"results": [], "page": f"{page} of 1", "total_results": 0, "total_pages": 1}), 200
        data = response.json()
        results = data.get("results", [])
        total_results = min(data.get("total_results", 0), MAX_TMDB_RESULTS)
        total_pages = min(data.get("total_pages", 1), MAX_TMDB_PAGES)
        items = [format_media_light(item, media_type) for item in results]
        return jsonify({
            "results": [i for i in items if i],
            "page": f"{page} of {total_pages}",
            "total_results": total_results,
            "total_pages": total_pages
        })
    except Exception as e:
        logger.error(f"Error in get_media_trending: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/<media_type>/discover', methods=['GET'])
@cached_with_stats(timeout=3600, key_prefix=lambda: f"{request.view_args['media_type']}_discover_{request.query_string.decode('utf-8')}")
def get_media_discover(media_type):
    if media_type not in ["movie", "tv"]:
        logger.warning(f"Invalid media_type: {media_type}")
        return jsonify({"error": "Invalid media type"}), 400
    page = request.args.get('page', 1, type=int)
    genre = request.args.get('genre')
    year = request.args.get('year')
    country = request.args.get('country')
    language = request.args.get('language')
    vote_average_gte = request.args.get('vote_average.gte', type=float)
    vote_average_lte = request.args.get('vote_average.lte', type=float)
    sort_by = request.args.get('sort_by', 'popularity.desc')
    sort_by = validate_sort_by(sort_by)
    logger.info(f"Discovering {media_type}s with filters: page={page}, genre={genre}, year={year}, country={country}, language={language}, vote_average.gte={vote_average_gte}, vote_average.lte={vote_average_lte}, sort_by={sort_by}")
    try:
        params = {"page": page, "sort_by": sort_by.replace("release_date", "primary_release_date" if media_type == "movie" else "first_air_date")}
        if genre:
            params["with_genres"] = genre
        if year:
            params["primary_release_year" if media_type == "movie" else "first_air_date_year"] = year
        if country:
            params["with_origin_country"] = country.upper()
        if language:
            params["with_original_language"] = language
        if vote_average_gte:
            params["vote_average.gte"] = vote_average_gte
        if vote_average_lte:
            params["vote_average.lte"] = vote_average_lte
        results = discover_api.discover_movies(params) if media_type == "movie" else discover_api.discover_tv_shows(params)
        if not results:
            logger.warning(f"No {media_type}s found with given filters")
            return jsonify({"results": [], "page": f"{page} of 1", "total_results": 0, "total_pages": 1}), 200
        total_results = min(results.total_results, MAX_TMDB_RESULTS)
        total_pages = min(results.total_pages, MAX_TMDB_PAGES)
        items = [format_media_light(item, media_type) for item in results]
        return jsonify({
            "results": [i for i in items if i],
            "page": f"{page} of {total_pages}",
            "total_results": total_results,
            "total_pages": total_pages
        })
    except Exception as e:
        logger.error(f"Error in get_media_discover: {str(e)}", exc_info=True)
        return jsonify({"error": "Internal server error"}), 500