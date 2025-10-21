//
// Copyright Metaspex - 2023
// mailto:admin@metaspex.com
//

#include <time.h>

#include "hx2a/root.hpp" // Points of interest are document roots.
#include "hx2a/components/position.hpp" // For the position type offering latitude and longitude.
#include "hx2a/slot.hpp" // A point of interest has a name.
#include "hx2a/own.hpp" // A point of interest owns a position.
#include "hx2a/own_list.hpp" // The payload returning points of interest bears a own list.
#include "hx2a/kdcache.hpp" // Using Metaspex's kdcache container.
#include "hx2a/service.hpp"
#include "hx2a/exception.hpp"
#include "hx2a/db/connector.hpp"

#include "hx2a/components/area.hpp" // Foundation Ontology type offering a rectangular latitude and longitude area.

#include "hx2a/payloads/query_id.hpp" // To receive a service payload giving a document identifier.
#include "hx2a/payloads/reply_id.hpp" // To reply a document identifier when creating a point of interest.

/*
  The code below is a Metaspex specification. It is also code. It is a specification in the sense that it essentially describes
  the "what" and not the "how". It is code in the sense that it is not a set of diagrams, it is a piece of formal text which is 
  validated and compiled using an off the shelf compiler.

  The specification below describes the "what" in the sense that it stands very high in terms of abstraction and is very precise,
  even more so than classical relational database schemas. We remove toil to reach multiple orders of magnitude of productivity
  boost.

  Below, you'll find:

  - No SQL and more generally no query language of any kind. The specification is entirely object-oriented, no cross-cut of the
    data model is made. Types in the ontology are completely reusable. They constitute "knowledge acquisition".
  - No low-level database-related code is present, no trace of Couchbase, MongoDB or CouchDB drivers, although all of these products
    are supported to make the information persistent.
    The mapping of logical database names below to physical databases is described in the configuration file. The object file produced 
    from the specification below is independent from physical databases and can be deployed on multiple databases.
  - No explicit commit or rollback is present.
  - No network-related code is present. Yet, the code below produces HTTP REST JSON in/JSON out services, optionally supporting
    TLS or SSL.
  - No reference is made to Apache or Nginx, yet the code below is compiled as Apache or Nginx plug-ins.
  - No password is specified, the binary produced is independent from them. They are exclusively in the configuration file(s).
  - No data presentation is specified, no JSON, no BSON (MongoDB). This is automatic.
  - No error-prone serialization/deserialization for the protocols above are specified. They are automatically generated.
  - No explicit objects memory deletion is present, objects collection is done automatically in a way faster than garbage-collected
    programming languages do it, and even faster than generalized reference counting techniques. Metaspex allows cycles.
  - Referential integrity is verified at compile time and dynamically also. It is also actively maintained without any code.

  It can be helpful to have Metaspex's reference guide open to understand each of the constructs used.
 */

using namespace hx2a; // To avoid prefixing everything with Metaspex's hx2a.

namespace poi {

  // Ontology.

  class poi;
  using poi_p = ptr<poi>;
  using poi_r = rfr<poi>;

  // We could add an address to a POI, taking the Foundation Ontology reusable address.
  class poi: public root<>
  {
    HX2A_ROOT(poi, "poi", 1, root,
	      (name, pos, category));
  public:

    // We could have derived types or more flexible, a separate category type a poi bears a strong link
    // to (that way newly-created categories could be dynamically created). Making it light for now.
    // Numbering explicitly so that evolutions are stable. If we add/subtract categories, the existing
    // poi documents with other categories will be fine.
    enum category_t {
		     ev_charging = 0,
		     landmark = 1,
		     museum = 2,
		     restaurant = 3,
		     shopping = 4
    };
    
    poi(string n, const position_r& p, category_t c):
      name(*this, n),
      pos(*this, p), // own accepts position_r.
      category(*this, c)
    {
    }

    static double get_latitude(const poi& p){ return p.pos->get_latitude(); }
    static double get_longitude(const poi& p){ return p.pos->get_longitude(); }
    static category_t get_category(const poi& p){ return p.category; }

    static constexpr tag_t index_by_last_save_timestamp = "poi_by_lst";
    
    slot<string, "name"> name;
    own<position, "pos"> pos; // The position type comes from Metaspex's Foundation Ontology.
    slot<category_t, "category"> category;
  };

  // Definition of the index type, using a Metaspex kdcache.
  // We add the last save timestamp of the most recent document, to allow for refresh.
  using poi_index = kdcache< // This ensures multithread safety.
    poi, // This is the type we obtain the keys from.
    slice_g<poi, double, poi::get_latitude>,
    slice_g<poi, double, poi::get_longitude>,
    slice_g<poi, poi::category_t, poi::get_category>
    >;

  // Function to build the index from a database cursor. It assumes that an index capable of scanning 
  // all points of interest exists (with the logical name "poi_per_lst" defined in the configuration file).
  inline poi_index& get_poi_index(const db::connector& cn){
    // Statics are thread-safe.
    static poi_index c(
		       "poi kdcache",                     // Name of the kdcache, just for tracing purposes.
		       cn,
		       poi::index_by_last_save_timestamp, // Name of the index by last save timestamp.
		       128,                               // Number of documents acquired by the cursor at build or refresh.
		       10                                 // Number of seconds before a new poi appears in the kdcache.
		       );
    return c;
  }

  // Service paylods.

  // A reusable base class for poi payloads.
  class poi_data_payload: public element<>
  {
    HX2A_ELEMENT(poi_data_payload, "poi_data_pld", element,
		 (name, pos));
  public:

    poi_data_payload(const poi_r& p):
      name(*this, p->name),
      pos(*this, p->pos->copy()) // The position is owned by the poi, we must copy it.
    {
    }

    slot<string, "name"> name;
    own<position, "position"> pos;
  };

  // Let's reuse and extend by adding the category.
  class poi_create_payload: public poi_data_payload
  {
    HX2A_ELEMENT(poi_create_payload, "poi_create_pld", poi_data_payload,
		 (category));
  public:

    slot<poi::category_t, "category"> category;
  };
 
  // We don't include the category, it is part of the search criteria, no need to return it.
  class poi_search_data_payload: public poi_data_payload
  {
    HX2A_ELEMENT(poi_search_data_payload, "poi_search_data_pld", poi_data_payload,
		 (id));
  public:

    poi_search_data_payload(const poi_r& p):
      poi_data_payload(p),
      id(*this, p->get_id())
    {
    }
    
    slot<doc_id, "id"> id;
  };

  class pois_search_data_payload: public element<>
  {
    HX2A_ELEMENT(pois_search_data_payload, "pois_search_data_pld", element,
		 (pois_data));
  public:

    pois_search_data_payload():
      pois_data(*this)
    {
    }

    void push_data(const rfr<poi_search_data_payload>& pd){
      pois_data.push_back(pd);
    }
    
    own_list<poi_search_data_payload, "pois"> pois_data;
  };

  // This is the type expected to search for a point of interest.
  // It contains the latitude and longitude rectangle and the category of poi.
  // We reuse the area type from Metaspex's Foundation Ontology.
  class area_and_category: public area
  {
    HX2A_ELEMENT(area_and_category, "area_and_category", area,
		 (category));
  public:

    area_and_category(
		      latitude_t latitude_min,
		      latitude_t latitude_max,
		      longitude_t longitude_min,
		      longitude_t longitude_max,
		      poi::category_t category
		  ):
      area(latitude_min, latitude_max, longitude_min, longitude_max),
      category(*this, category)
    {
    }
    
    slot<poi::category_t, "category"> category;
  };

  // Application exceptions definitions.

  using position_is_missing = application_exception<"pmiss", "Position is missing.">;
  
  // Service definitions.

  // Creation of a POI.
  auto _poi_create = service<"poi_create"> // Singleton. // The name of the service as it'll appear at the end of the URI accepted by the server.
    ([](const rfr<poi_create_payload>& pcp) -> reply_id_p {
      db::connector c{"hx2a"}; // Connector to the database described in the configuration file under the logical name "hx2a".
      
      // A poi constructor takes a non-null position, we must make sure the client did not forget to send one.
      
      // Throwing an exception will stop the service and send back a graceful error message to the client, with specifics about
      // the issue.
      position_r pcppos = pcp->pos.or_throw<position_is_missing>();
      
      // Creation of the poi. As we have a connector it'll persist in the "hx2a" database.
      // We could write the two lines below as a single one. Using two for readability.
      poi_r point = make<poi>(*c, pcp->name, pcppos->copy(), pcp->category);
      
      // Returning the document identifier of the newly-created poi to the client.
      return make<reply_id>(point->get_id());
      
      // No commit, it's done automatically by Metaspex at the end of a successful service call (without exception thrown).
    });

  // Deletion of a poi.
  auto _poi_delete = service<"poi_delete">
    ([](const rfr<query_id>& q){
      db::connector c{"hx2a"};

      // Retrieving the point of interest. It is a ptr and not a rfr because the document might not exist and get will return null.
      poi_r point = poi::get(c, q->get_id()).or_throw<document_does_not_exist>();
      
      // This marks the document for removal, except if a rollback happens before the end of the service. A rollback is automatically
      // triggered in case of exception. As we return right after, the document will be removed.
      point->unpublish();
    });

  // Searching for a POI within an area and a given category.
  auto _poi_search = service<"poi_search">
    ([](const rfr<area_and_category>& query) -> ptr<pois_search_data_payload> {
      db::connector c{"hx2a"};
      // Grabbing the index. The first time it will build it.
      poi_index& pi = get_poi_index(c);
      // We want to display max 100 pois.
      // We add one so that if we find 101, we return nothing so that the user has to zoom in.
      constexpr size_t search_limit = 100 + 1;
      // Preparing an array (could be another container such as std::vector or a std::deque) to store the search results.
      std::array<poi_p, search_limit> a;
      auto i = a.begin();
      // Obtaining the intervals from the area payload.
      // Putting them aside in case we reuse them for erasure.
      interval<double> li = query->get_latitude_interval();
      interval<double> Li = query->get_longitude_interval();
      // The category interval is a singleton.
      interval<poi::category_t> ti{query->category};
      // Searching in the index.
      auto e = pi.search(i, search_limit, li, Li, ti);
      
      // We count how many pois we found.
      // If we got what we asked for (101 pois), we return nothing. This is different from returning an empty list.
      // It means that the user must zoom in.
      if (size_t(e - i) == search_limit){
	return {}; // Please zoom in. Too much to display.
      }
      
      // Now we can get the documents (if any).
      // Building the empty reply.
      rfr<pois_search_data_payload> pdp = make<pois_search_data_payload>();
      
      // Scanning all the search results.
      while (i != e){
	pdp->push_data(make<poi_search_data_payload>(**i));
	++i;
      }
      
      // Returning the payload. If nothing was found the JSON reply will contain an empty array of pois.
      return pdp;
    });
  
} // End namespace poi.
