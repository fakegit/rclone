// Package googlephotos provides an interface to Google Photos
package googlephotos

/*

For duplicates, prefix the file name with {theID} then can find those easily

FIXME album names could have / in
FIXME image names might have / in too ?

/upload directory can show all recent uploads?

Is creation date the time of upload or EXIF time or what? It is the EXIF creation time.

IDs are base64 [A-Za-z0-9_-], shortest seen is 55 chars
[A-Za-z0-9_-]{55,}

*/

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ncw/rclone/backend/googlephotos/api"
	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/fs/config"
	"github.com/ncw/rclone/fs/config/configmap"
	"github.com/ncw/rclone/fs/config/configstruct"
	"github.com/ncw/rclone/fs/config/obscure"
	"github.com/ncw/rclone/fs/fserrors"
	"github.com/ncw/rclone/fs/hash"
	"github.com/ncw/rclone/lib/oauthutil"
	"github.com/ncw/rclone/lib/pacer"
	"github.com/ncw/rclone/lib/rest"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

var (
	errCantUpload  = errors.New("can't upload files here")
	errCantMkdir   = errors.New("can't make directories here")
	errCantRmdir   = errors.New("can't remove this directory")
	errAlbumDelete = errors.New("google photos API does not implement deleting albums")
	errRemove      = errors.New("google photos API does not implement deleting files")
	errOwnAlbums   = errors.New("google photos API only allows uploading to albums rclone created")
)

const (
	rcloneClientID              = "202264815644-rt1o1c9evjaotbpbab10m83i8cnjk077.apps.googleusercontent.com"
	rcloneEncryptedClientSecret = "kLJLretPefBgrDHosdml_nlF64HZ9mUcO85X5rdjYBPP8ChA-jr3Ow"
	rootURL                     = "https://photoslibrary.googleapis.com/v1"
	listChunks                  = 100 // chunk size to read directory listings
	albumChunks                 = 50  // chunk size to read album listings
	minSleep                    = 10 * time.Millisecond
	maxSleep                    = 2 * time.Second
	decayConstant               = 2 // bigger for slower decay, exponential
)

var (
	// Description of how to auth for this app
	oauthConfig = &oauth2.Config{
		Scopes: []string{
			// https://www.googleapis.com/auth/photoslibrary.readonly
			"https://www.googleapis.com/auth/photoslibrary",
		},
		Endpoint:     google.Endpoint,
		ClientID:     rcloneClientID,
		ClientSecret: obscure.MustReveal(rcloneEncryptedClientSecret),
		RedirectURL:  oauthutil.TitleBarRedirectURL,
	}
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "google photos",
		Prefix:      "gphotos",
		Description: "Google Photos",
		NewFs:       NewFs,
		Config: func(name string, m configmap.Mapper) {
			err := oauthutil.Config("google photos", name, m, oauthConfig)
			if err != nil {
				log.Fatalf("Failed to configure token: %v", err)
			}
		},
		Options: []fs.Option{{
			Name: config.ConfigClientID,
			Help: "Google Application Client Id\nLeave blank normally.",
		}, {
			Name: config.ConfigClientSecret,
			Help: "Google Application Client Secret\nLeave blank normally.",
		}, {
			Name:    "read_size",
			Default: false,
			Help: `Set to read the size of media items.

Normally rclone does not read the size of media items since this takes
another transaction.  This isn't necessary for syncing.  However
rclone mount needs to know the size of files in advance of reading
them, so setting this flag when using rclone mount is recommended if
you want to read the media.`,
		}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	ReadSize bool `config:"read_size"`
}

// Fs represents a remote storage server
type Fs struct {
	name      string           // name of this remote
	root      string           // the path we are working on if any
	opt       Options          // parsed options
	features  *fs.Features     // optional features
	srv       *rest.Client     // the connection to the one drive server
	pacer     *fs.Pacer        // To pace the API calls
	startTime time.Time        // time Fs was started - used for datestamps
	albums    map[bool]*albums // albums, shared or not
}

// All the albums
type albums struct {
	mu      sync.Mutex
	dupes   map[string][]*api.Album // duplicated names
	byID    map[string]*api.Album   //..indexed by ID
	byTitle map[string]*api.Album   //..indexed by Title
}

// Create a new album
func newAlbums() *albums {
	return &albums{
		dupes:   map[string][]*api.Album{},
		byID:    map[string]*api.Album{},
		byTitle: map[string]*api.Album{},
	}
}

// add an album
func (all *albums) add(album *api.Album) {
	all.mu.Lock()
	defer all.mu.Unlock()

	// Munge the name of the album
	album.Title = strings.Replace(album.Title, "/", "／", -1)

	// update dupes by title
	dupes := all.dupes[album.Title]
	dupes = append(dupes, album)
	all.dupes[album.Title] = dupes

	// Dedupe the album name if necessary
	if len(dupes) >= 2 {
		album.Title = addID(album.Title, album.ID)
		// If this is the first dupe, then need to adjust the first one
		if len(dupes) == 2 {
			firstAlbum := dupes[0]
			delete(all.byTitle, firstAlbum.Title) // remove old name
			firstAlbum.Title = addID(firstAlbum.Title, firstAlbum.ID)
			all.byTitle[firstAlbum.Title] = firstAlbum // add new name
		}
	}

	// Store the new album
	all.byID[album.ID] = album
	all.byTitle[album.Title] = album
}

// get an album by title
func (all *albums) get(title string) (album *api.Album, ok bool) {
	all.mu.Lock()
	defer all.mu.Unlock()
	album, ok = all.byTitle[title]
	return album, ok
}

// Object describes a storage object
//
// Will definitely have info but maybe not meta
type Object struct {
	fs       *Fs       // what this object is part of
	remote   string    // The remote path
	url      string    // download path
	id       string    // ID of this object
	bytes    int64     // Bytes in the object
	modTime  time.Time // Modified time of the object
	mimeType string
}

// ------------------------------------------------------------

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("Google Photos path %s", f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// retryErrorCodes is a slice of error codes that we will retry
var retryErrorCodes = []int{
	429, // Too Many Requests.
	500, // Internal Server Error
	502, // Bad Gateway
	503, // Service Unavailable
	504, // Gateway Timeout
	509, // Bandwidth Limit Exceeded
}

// shouldRetry returns a boolean as to whether this resp and err
// deserve to be retried.  It returns the err as a convenience
func shouldRetry(resp *http.Response, err error) (bool, error) {
	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

// errorHandler parses a non 2xx error response into an error
func errorHandler(resp *http.Response) error {
	body, err := rest.ReadBody(resp)
	if err != nil {
		body = nil
	}
	var e = api.Error{
		Details: api.ErrorDetails{
			Code:    resp.StatusCode,
			Message: string(body),
			Status:  resp.Status,
		},
	}
	if body != nil {
		_ = json.Unmarshal(body, &e)
	}
	return &e
}

// NewFs constructs an Fs from the path, bucket:path
func NewFs(name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	oAuthClient, _, err := oauthutil.NewClient(name, m, oauthConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure Box")
	}

	root = path.Clean(root)
	if root == "." {
		root = ""
	}
	f := &Fs{
		name:      name,
		root:      root,
		opt:       *opt,
		srv:       rest.NewClient(oAuthClient).SetRoot(rootURL),
		pacer:     fs.NewPacer(pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
		startTime: time.Now(),
		albums:    map[bool]*albums{},
	}
	f.features = (&fs.Features{
		ReadMimeType:  true,
		WriteMimeType: true,
	}).Fill(f)
	f.srv.SetErrorHandler(errorHandler)

	_, _, pattern := patterns.match(f.root, "")
	if pattern != nil && pattern.isFile {
		f.root, _ = path.Split(f.root)
		f.root = strings.Trim(f.root, "/")
		return f, fs.ErrorIsFile
	}

	return f, nil
}

// Return an Object from a path
//
// If it can't be found it returns the error fs.ErrorObjectNotFound.
func (f *Fs) newObjectWithInfo(ctx context.Context, remote string, info *api.MediaItem) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}
	if info != nil {
		o.setMetaData(info)
	} else {
		err := o.readMetaData(ctx) // reads info and meta, returning an error
		if err != nil {
			return nil, err
		}
	}
	return o, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return f.newObjectWithInfo(ctx, remote, nil)
}

// addID adds the ID to name
func addID(name string, ID string) string {
	idStr := "{" + ID + "}"
	if name == "" {
		return idStr
	}
	return name + " " + idStr
}

// addFileID adds the ID to the fileName passed in
func addFileID(fileName string, ID string) string {
	ext := path.Ext(fileName)
	base := fileName[:len(fileName)-len(ext)]
	return addID(base, ID) + ext
}

var idRe = regexp.MustCompile(`\{([A-Za-z0-9_-]{55,})\}`)

// findID finds an ID in string if one is there or ""
func findID(name string) string {
	match := idRe.FindStringSubmatch(name)
	if match == nil {
		return ""
	}
	return match[1]
}

// list the albums into an internal cache
// FIXME cache invalidation
func (f *Fs) listAlbums(shared bool) (all *albums, err error) {
	all, ok := f.albums[shared]
	if ok && all != nil {
		return all, nil
	}
	opts := rest.Opts{
		Method:     "GET",
		Path:       "/albums",
		Parameters: url.Values{},
	}
	if shared {
		opts.Path = "/sharedAlbums"
	}
	all = newAlbums()
	opts.Parameters.Set("pageSize", strconv.Itoa(albumChunks))
	lastID := ""
	for {
		var result api.ListAlbums
		var resp *http.Response
		err = f.pacer.Call(func() (bool, error) {
			resp, err = f.srv.CallJSON(&opts, nil, &result)
			return shouldRetry(resp, err)
		})
		if err != nil {
			return nil, errors.Wrap(err, "couldn't list albums")
		}
		newAlbums := result.Albums
		if shared {
			newAlbums = result.SharedAlbums
		}
		if len(newAlbums) > 0 && newAlbums[0].ID == lastID {
			// skip first if ID duplicated from last page
			newAlbums = newAlbums[1:]
		}
		if len(newAlbums) > 0 {
			lastID = newAlbums[len(newAlbums)-1].ID
		}
		for i := range newAlbums {
			all.add(&newAlbums[i])
		}
		if result.NextPageToken == "" {
			break
		}
		opts.Parameters.Set("pageToken", result.NextPageToken)
	}
	f.albums[shared] = all
	return all, nil
}

// listFn is called from list to handle an object.
type listFn func(remote string, object *api.MediaItem, isDirectory bool) error

// list the objects into the function supplied
//
// dir is the starting directory, "" for root
//
// Set recurse to read sub directories
func (f *Fs) list(dir string, filter api.SearchFilter, fn listFn) (err error) {
	opts := rest.Opts{
		Method: "POST",
		Path:   "/mediaItems:search",
	}
	filter.PageSize = listChunks
	filter.PageToken = ""
	lastID := ""
	for {
		var result api.MediaItems
		var resp *http.Response
		err = f.pacer.Call(func() (bool, error) {
			resp, err = f.srv.CallJSON(&opts, &filter, &result)
			return shouldRetry(resp, err)
		})
		if err != nil {
			return errors.Wrap(err, "couldn't list files")
		}
		items := result.MediaItems
		if len(items) > 0 && items[0].ID == lastID {
			// skip first if ID duplicated from last page
			items = items[1:]
		}
		if len(items) > 0 {
			lastID = items[len(items)-1].ID
		}
		for i := range items {
			item := &result.MediaItems[i]
			remote := item.Filename
			remote = strings.Replace(remote, "/", "／", -1)
			err = fn(remote, item, false)
			if err != nil {
				return err
			}
		}
		if result.NextPageToken == "" {
			break
		}
		filter.PageToken = result.NextPageToken
	}

	return nil
}

// Convert a list item into a DirEntry
func (f *Fs) itemToDirEntry(ctx context.Context, remote string, item *api.MediaItem, isDirectory bool) (fs.DirEntry, error) {
	if isDirectory {
		d := fs.NewDir(remote, f.startTime)
		return d, nil
	}
	o, err := f.newObjectWithInfo(ctx, remote, item)
	if err != nil {
		return nil, err
	}
	return o, nil
}

// listDir lists a single directory
func (f *Fs) listDir(ctx context.Context, path string, dir string, filter api.SearchFilter) (entries fs.DirEntries, err error) {
	// List the objects
	err = f.list(dir, filter, func(remote string, item *api.MediaItem, isDirectory bool) error {
		entry, err := f.itemToDirEntry(ctx, path+remote, item, isDirectory)
		if err != nil {
			return err
		}
		if entry != nil {
			entries = append(entries, entry)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Dedupe the file names
	dupes := map[string]int{}
	for _, entry := range entries {
		o, ok := entry.(*Object)
		if ok {
			dupes[o.remote]++
		}
	}
	for _, entry := range entries {
		o, ok := entry.(*Object)
		if ok {
			duplicated := dupes[o.remote] > 1
			if duplicated || o.remote == "" {
				o.remote = addFileID(o.remote, o.id)
			}
		}
	}
	return entries, err
}

// dirPattern describes a single directory pattern
type dirPattern struct {
	re        string
	match     *regexp.Regexp
	canUpload bool
	canMkdir  bool
	isFile    bool
	toDirs    func(f *Fs, absPath string, match []string) (fs.DirEntries, error)
	toFilter  func(f *Fs, match []string) (api.SearchFilter, error)
}

// dirPatters is a slice of all the directory patterns
type dirPatterns []dirPattern

// mustCompile compiles the regexps in the dirPatterns
func (ds dirPatterns) mustCompile() dirPatterns {
	for i := range ds {
		pattern := &ds[i]
		pattern.match = regexp.MustCompile(pattern.re)
	}
	return ds
}

// match finds the path passed in in the matching structure and
// returns the parameters and a pointer to the match, or nil.
func (ds dirPatterns) match(root string, itemPath string) (match []string, prefix string, pattern *dirPattern) {
	itemPath = strings.Trim(itemPath, "/")
	absPath := path.Join(root, itemPath)
	prefix = strings.Trim(absPath[len(root):], "/")
	if prefix != "" {
		prefix += "/"
	}
	for i := range ds {
		pattern = &ds[i]
		match = pattern.match.FindStringSubmatch(absPath)
		if match != nil {
			return
		}
	}
	return nil, "", nil
}

// Return the years from 2000 to today
// FIXME make configurable?
func years(f *Fs, absPath string, match []string) (entries fs.DirEntries, err error) {
	currentYear := time.Now().Year()
	for year := 2000; year <= currentYear; year++ {
		entries = append(entries, fs.NewDir(absPath+fmt.Sprint(year), f.startTime))
	}
	return entries, nil
}

// Return the months in a given year
func months(f *Fs, absPath string, match []string) (entries fs.DirEntries, err error) {
	year := match[1]
	for month := 1; month <= 12; month++ {
		entries = append(entries, fs.NewDir(fmt.Sprintf("%s%s-%02d", absPath, year, month), f.startTime))
	}
	return entries, nil
}

// Return the days in a given year
func days(f *Fs, absPath string, match []string) (entries fs.DirEntries, err error) {
	year := match[1]
	current, err := time.Parse("2006", year)
	if err != nil {
		return nil, errors.Errorf("bad year %q", match[1])
	}
	currentYear := current.Year()
	for current.Year() == currentYear {
		entries = append(entries, fs.NewDir(absPath+current.Format("2006-01-02"), f.startTime))
		current = current.AddDate(0, 0, 1)
	}
	return entries, nil
}

// This filters on year/month/day as provided
func yearMonthDayFilter(f *Fs, match []string) (sf api.SearchFilter, err error) {
	year, err := strconv.Atoi(match[1])
	if err != nil || year < 1000 || year > 3000 {
		return sf, errors.Errorf("bad year %q", match[1])
	}
	sf = api.SearchFilter{
		Filters: &api.Filters{
			DateFilter: &api.DateFilter{
				Dates: []api.Date{
					{
						Year: year,
					},
				},
			},
		},
	}
	if len(match) >= 3 {
		month, err := strconv.Atoi(match[2])
		if err != nil || month < 1 || month > 12 {
			return sf, errors.Errorf("bad month %q", match[2])
		}
		sf.Filters.DateFilter.Dates[0].Month = month
	}
	if len(match) >= 4 {
		day, err := strconv.Atoi(match[3])
		if err != nil || day < 1 || day > 31 {
			return sf, errors.Errorf("bad day %q", match[3])
		}
		sf.Filters.DateFilter.Dates[0].Day = day
	}
	return sf, nil
}

// Turns the albums into directories
func albumsToDirs(f *Fs, shared bool, absPath string, match []string) (entries fs.DirEntries, err error) {
	albums, err := f.listAlbums(shared)
	if err != nil {
		return nil, err
	}
	for _, album := range albums.byID {
		count, err := strconv.ParseInt(album.MediaItemsCount, 10, 64)
		if err != nil {
			fs.Debugf(f, "Error reading media count: %v", err)
		}
		entries = append(entries, fs.NewDir(absPath+album.Title, f.startTime).SetID(album.ID).SetItems(count))
	}
	return entries, nil
}

// Turns albums into search filter
func albumsToFilter(f *Fs, shared bool, match []string) (sf api.SearchFilter, err error) {
	albumName := match[1]
	id := findID(albumName)
	if id == "" {
		albums, err := f.listAlbums(shared)
		if err != nil {
			return sf, err
		}
		album, ok := albums.byTitle[albumName]
		if !ok {
			return sf, fs.ErrorDirNotFound
		}
		id = album.ID
	}
	return api.SearchFilter{AlbumID: id}, nil
}

// No trailing /
var patterns = dirPatterns{
	{
		re: `^$`,
		toDirs: func(f *Fs, absPath string, match []string) (fs.DirEntries, error) {
			return fs.DirEntries{
				fs.NewDir(absPath+"media", f.startTime),
				fs.NewDir(absPath+"album", f.startTime),
				fs.NewDir(absPath+"shared-album", f.startTime),
				fs.NewDir(absPath+"upload", f.startTime),
			}, nil
		},
	},
	{
		re: `^upload$`,
		toDirs: func(f *Fs, absPath string, match []string) (fs.DirEntries, error) {
			return fs.DirEntries{}, nil
		},
	},
	// {
	// 	re:        `^upload/([^/]+)$`,
	// 	canUpload: true,
	// 	isFile:    true,
	// },
	{
		re: `^media$`,
		toDirs: func(f *Fs, absPath string, match []string) (fs.DirEntries, error) {
			return fs.DirEntries{
				fs.NewDir(absPath+"all", f.startTime),
				fs.NewDir(absPath+"by-year", f.startTime),
				fs.NewDir(absPath+"by-month", f.startTime),
				fs.NewDir(absPath+"by-day", f.startTime),
			}, nil
		},
	},
	{
		re: `^media/all$`,
		toFilter: func(f *Fs, match []string) (sf api.SearchFilter, err error) {
			return sf, nil
		},
	},
	{
		re:     `^media/all/([^/]+)$`,
		isFile: true,
	},
	{
		re:     `^media/by-year$`,
		toDirs: years,
	},
	{
		re:       `^media/by-year/(\d{4})$`,
		toFilter: yearMonthDayFilter,
	},
	{
		re:     `^media/by-year/(\d{4})/([^/]+)$`,
		isFile: true,
	},
	{
		re:     `^media/by-month$`,
		toDirs: years,
	},
	{
		re:     `^media/by-month/(\d{4})$`,
		toDirs: months,
	},
	{
		re:       `^media/by-month/\d{4}/(\d{4})-(\d{2})$`,
		toFilter: yearMonthDayFilter,
	},
	{
		re:     `^media/by-month/\d{4}/(\d{4})-(\d{2})/([^/]+)$`,
		isFile: true,
	},
	{
		re:     `^media/by-day$`,
		toDirs: years,
	},
	{
		re:     `^media/by-day/(\d{4})$`,
		toDirs: days,
	},
	{
		re:       `^media/by-day/\d{4}/(\d{4})-(\d{2})-(\d{2})$`,
		toFilter: yearMonthDayFilter,
	},
	{
		re:     `^media/by-day/\d{4}/(\d{4})-(\d{2})-(\d{2})/([^/]+)$`,
		isFile: true,
	},
	{
		re: `^album$`,
		toDirs: func(f *Fs, absPath string, match []string) (entries fs.DirEntries, err error) {
			return albumsToDirs(f, false, absPath, match)
		},
	},
	{
		re:       `^album/([^/]+)$`,
		canMkdir: true,
		toFilter: func(f *Fs, match []string) (sf api.SearchFilter, err error) {
			return albumsToFilter(f, false, match)

		},
	},
	{
		re:        `^album/([^/]+)/([^/]+)$`,
		canUpload: true,
		isFile:    true,
	},
	{
		re: `^shared-album$`,
		toDirs: func(f *Fs, absPath string, match []string) (entries fs.DirEntries, err error) {
			return albumsToDirs(f, true, absPath, match)
		},
	},
	{
		re: `^shared-album/([^/]+)$`,
		toFilter: func(f *Fs, match []string) (sf api.SearchFilter, err error) {
			return albumsToFilter(f, true, match)

		},
	},
	{
		re:     `^shared-album/([^/]+)/([^/]+)$`,
		isFile: true,
	},
}.mustCompile()

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	match, prefix, pattern := patterns.match(f.root, dir)
	if pattern == nil || pattern.isFile {
		return nil, fs.ErrorDirNotFound
	}
	if pattern.toDirs != nil {
		return pattern.toDirs(f, prefix, match)
	}
	if pattern.toFilter != nil {
		filter, err := pattern.toFilter(f, match)
		if err != nil {
			return nil, errors.Wrapf(err, "bad filter when listing %q", dir)
		}
		return f.listDir(ctx, prefix, dir, filter)
	}
	return nil, fs.ErrorDirNotFound
}

// Put the object into the bucket
//
// Copy the reader in to the new object which is returned
//
// The new object may have been created if an error is returned
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	// Temporary Object under construction
	o := &Object{
		fs:     f,
		remote: src.Remote(),
	}
	return o, o.Update(ctx, in, src, options...)
}

// createAlbum creates the album
func (f *Fs) createAlbum(ctx context.Context, albumName string) (album *api.Album, err error) {
	opts := rest.Opts{
		Method:     "POST",
		Path:       "/albums",
		Parameters: url.Values{},
	}
	var request = api.CreateAlbum{
		Album: &api.Album{
			Title: albumName,
		},
	}
	var result api.Album
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(&opts, request, &result)
		return shouldRetry(resp, err)
	})
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create album")
	}
	f.albums[false].add(&result)
	return album, nil
}

// Mkdir creates the album if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) (err error) {
	match, _, pattern := patterns.match(f.root, dir)
	if pattern == nil {
		return fs.ErrorDirNotFound
	}
	if !pattern.canMkdir {
		return errCantMkdir
	}
	albumName := match[1]
	allAlbums, err := f.listAlbums(false)
	if err != nil {
		return err
	}
	_, ok := allAlbums.get(albumName)
	if ok {
		return nil
	}
	_, err = f.createAlbum(ctx, albumName)
	return err
}

// Rmdir deletes the bucket if the fs is at the root
//
// Returns an error if it isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) (err error) {
	match, _, pattern := patterns.match(f.root, dir)
	if pattern == nil {
		return fs.ErrorDirNotFound
	}
	if !pattern.canMkdir {
		return errCantRmdir
	}
	albumName := match[1]
	allAlbums, err := f.listAlbums(false)
	if err != nil {
		return err
	}
	album, ok := allAlbums.get(albumName)
	if !ok {
		return fs.ErrorDirNotFound
	}
	_ = album
	return errAlbumDelete
}

// Precision returns the precision
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.None)
}

// ------------------------------------------------------------

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// Hash returns the Md5sum of an object returning a lowercase hex string
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	if !o.fs.opt.ReadSize || o.bytes >= 0 {
		return o.bytes
	}
	var resp *http.Response
	opts := rest.Opts{
		Method:  "HEAD",
		RootURL: o.downloadURL(),
	}
	var err error
	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = o.fs.srv.Call(&opts)
		return shouldRetry(resp, err)
	})
	if err != nil {
		fs.Debugf(o, "Reading size failed: %v", err)
	} else {
		lengthStr := resp.Header.Get("Content-Length")
		length, err := strconv.ParseInt(lengthStr, 10, 64)
		if err != nil {
			fs.Debugf(o, "Reading size failed to parse Content_length %q: %v", lengthStr, err)
		} else {
			o.bytes = length
		}
	}
	return o.bytes
}

// setMetaData sets the fs data from a storage.Object
func (o *Object) setMetaData(info *api.MediaItem) {
	o.url = info.BaseURL
	o.id = info.ID
	o.bytes = -1 // FIXME
	o.mimeType = info.MimeType
	o.modTime = info.MediaMetadata.CreationTime
}

// readMetaData gets the metadata if it hasn't already been fetched
//
// it also sets the info
func (o *Object) readMetaData(ctx context.Context) (err error) {
	if !o.modTime.IsZero() {
		return nil
	}
	dir, fileName := path.Split(o.remote)
	dir = strings.Trim(dir, "/")
	_, _, pattern := patterns.match(o.fs.root, o.remote)
	if pattern == nil {
		return fs.ErrorObjectNotFound
	}
	if !pattern.isFile {
		return fs.ErrorNotAFile
	}
	// If have ID fetch it directly
	if id := findID(fileName); id != "" {
		opts := rest.Opts{
			Method: "GET",
			Path:   "/mediaItems/" + id,
		}
		var item api.MediaItem
		var resp *http.Response
		err = o.fs.pacer.Call(func() (bool, error) {
			resp, err = o.fs.srv.CallJSON(&opts, nil, &item)
			return shouldRetry(resp, err)
		})
		if err != nil {
			return errors.Wrap(err, "couldn't get media item")
		}
		o.setMetaData(&item)
		return nil
	}
	// Otherwise list the directory the file is in
	entries, err := o.fs.List(ctx, dir)
	if err != nil {
		return err
	}
	// and find the file in the directory
	for _, entry := range entries {
		if entry.Remote() == o.remote {
			if newO, ok := entry.(*Object); ok {
				*o = *newO
				return nil
			}
		}
	}
	return fs.ErrorObjectNotFound
}

// ModTime returns the modification time of the object
//
// It attempts to read the objects mtime and if that isn't present the
// LastModified returned in the http headers
func (o *Object) ModTime(ctx context.Context) time.Time {
	err := o.readMetaData(ctx)
	if err != nil {
		// fs.Logf(o, "Failed to read metadata: %v", err)
		return time.Now()
	}
	return o.modTime
}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) (err error) {
	return fs.ErrorCantSetModTime
}

// Storable returns a boolean as to whether this object is storable
func (o *Object) Storable() bool {
	return true
}

// downloadURL returns the URL for a full bytes download for the object
func (o *Object) downloadURL() string {
	url := o.url + "=d"
	if strings.HasPrefix(o.mimeType, "video/") {
		url += "v"
	}
	return url
}

// Open an object for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	var resp *http.Response
	opts := rest.Opts{
		Method:  "GET",
		RootURL: o.downloadURL(),
		Options: options,
	}
	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = o.fs.srv.Call(&opts)
		return shouldRetry(resp, err)
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, err
}

// Update the object with the contents of the io.Reader, modTime and size
//
// The new object may have been created if an error is returned
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	match, _, pattern := patterns.match(o.fs.root, o.remote)
	if pattern == nil || !pattern.isFile || !pattern.canUpload {
		return errCantUpload
	}
	albumName, fileName := match[1], match[2]
	fileName = strings.Replace(fileName, "/", "／", -1)

	// Create album if not found
	album, ok := o.fs.albums[false].get(albumName)
	if !ok {
		album, err = o.fs.createAlbum(ctx, albumName)
		if err != nil {
			return err
		}
	}

	// Check we can write to this album
	if !album.IsWriteable {
		return errOwnAlbums
	}

	// Upload the media item in exchange for an UploadToken
	opts := rest.Opts{
		Method: "POST",
		Path:   "/uploads",
		ExtraHeaders: map[string]string{
			"X-Goog-Upload-File-Name": fileName,
			"X-Goog-Upload-Protocol":  "raw",
		},
		Body: in,
	}
	var token []byte
	var resp *http.Response
	err = o.fs.pacer.CallNoRetry(func() (bool, error) {
		resp, err = o.fs.srv.Call(&opts)
		if err != nil {
			_ = resp.Body.Close()
			return shouldRetry(resp, err)
		}
		token, err = rest.ReadBody(resp)
		return shouldRetry(resp, err)
	})
	if err != nil {
		return errors.Wrap(err, "couldn't upload file")
	}
	uploadToken := strings.TrimSpace(string(token))
	if uploadToken == "" {
		return errors.New("empty upload token")
	}

	// Add the media item to the album using the UploadToken
	opts = rest.Opts{
		Method: "POST",
		Path:   "/mediaItems:batchCreate",
	}
	var request = api.BatchCreateRequest{
		AlbumID: album.ID,
		NewMediaItems: []api.NewMediaItem{
			{
				SimpleMediaItem: api.SimpleMediaItem{
					UploadToken: uploadToken,
				},
			},
		},
	}
	var result api.BatchCreateResponse
	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = o.fs.srv.CallJSON(&opts, request, &result)
		return shouldRetry(resp, err)
	})
	if err != nil {
		return errors.Wrap(err, "couldn't create album")
	}
	if len(result.NewMediaItemResults) != 1 {
		return errors.New("bad response to BatchCreate wrong number of items")
	}
	mediaItemResult := result.NewMediaItemResults[0]
	if resp.StatusCode == http.StatusMultiStatus {
		return errors.Errorf("upload failed with error %s (%d)", mediaItemResult.Status.Message, mediaItemResult.Status.Code)
	}
	o.setMetaData(&mediaItemResult.MediaItem)
	return nil
}

// Remove an object
func (o *Object) Remove(ctx context.Context) (err error) {
	return errRemove
}

// MimeType of an Object if known, "" otherwise
func (o *Object) MimeType(ctx context.Context) string {
	return o.mimeType
}

// ID of an Object if known, "" otherwise
func (o *Object) ID() string {
	return o.id
}

// Check the interfaces are satisfied
var (
	_ fs.Fs        = &Fs{}
	_ fs.Object    = &Object{}
	_ fs.MimeTyper = &Object{}
	_ fs.IDer      = &Object{}
)
