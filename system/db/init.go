package db

import (
	"log"

	"github.com/nilslice/jwt"
	"github.com/ponzu-cms/ponzu/system/item"
	"github.com/ponzu-cms/ponzu/system/search"
)

var (
	buckets = []string{
		"__config", "__users",
		"__addons", "__uploads",
		"__contentIndex",
	}

	bucketsToAdd 
	[]string
)

// Close exports the abillity to close our db file. Should be called with defer
// after call to Init() from the same place.
func Close() {
	err := store.Close()
	if err != nil {
		log.Println(err)
	}
}

// Init creates a db connection, initializes db with required info, sets secrets
func Init() {

	// init db with items and buckets as needed
	buckets = append(buckets, bucketsToAdd...)

	store, err := repo.NewBolt()

	if err != nil {
		log.Fatalln(err)
	}

	store.setRepository(repository)
	store.InitSchema(buckets)

	err = LoadCacheConfig()
	if err != nil {
		log.Fatalln("Failed to load config cache.", err)
	}

	clientSecret := ConfigCache("client_secret").(string)

	if clientSecret != "" {
		jwt.Secret([]byte(clientSecret))
	}

	// invalidate cache on system start
	err = InvalidateCache()
	if err != nil {
		log.Fatalln("Failed to invalidate cache.", err)
	}
}

// AddBucket adds a bucket to be created if it doesn't already exist
func AddBucket(name string) {
	bucketsToAdd = append(bucketsToAdd, name)
}

// InitSearchIndex initializes Search Index for search to be functional
// This was moved out of db.Init and put to main(), because addon checker was initializing db together with
// search indexing initialisation in time when there were no item.Types defined so search index was always
// empty when using addons. We still have no guarentee whatsoever that item.Types is defined
// Should be called from a goroutine after SetContent is successful (SortContent requirement)
func InitSearchIndex() {
	for t := range item.Types {
		err := search.MapIndex(t)
		if err != nil {
			log.Fatalln(err)
			return
		}
		SortContent(t)
	}
}

// SystemInitComplete checks if there is at least 1 admin user in the db which
// would indicate that the system has been configured to the minimum required.
func SystemInitComplete() bool {
	complete := true

	_, err := store.GetAll("__users")

	if err != nil {
		complete = false
		log.Fatalln(err)
	}

	return complete
}
