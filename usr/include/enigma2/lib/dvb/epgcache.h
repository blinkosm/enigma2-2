#ifndef __epgcache_h_
#define __epgcache_h_

//#define ENABLE_PRIVATE_EPG 1
//#define ENABLE_MHW_EPG 1

#ifndef SWIG

#include <vector>
#include <list>
#include <queue>
#if defined(__GXX_EXPERIMENTAL_CXX0X__)
#include <unordered_map>
#include <unordered_set>
#else
#include <ext/hash_map>
#include <ext/hash_set>
#endif

#include <errno.h>
#include <features.h>

#include <lib/dvb/idvb.h>
#include <lib/dvb/demux.h>
#include <lib/dvb/dvbtime.h>
#include <lib/base/ebase.h>
#include <lib/base/thread.h>
#include <lib/base/message.h>
#include <lib/service/event.h>
#include <lib/python/python.h>

#define CLEAN_INTERVAL 60000    //  1 min
#define UPDATE_INTERVAL 60000 // 1 min
#define ZAP_DELAY 2000          // 2 sek

#define HILO(x) (x##_hi << 8 | x##_lo)

class cacheData;
struct eServiceReferenceDVB;
class eDVBServicePMTHandler;

struct uniqueEPGKey
{
	int sid, onid, tsid, dvbnamespace;
	uniqueEPGKey( const eServiceReference &ref )
		:sid( ref.type != eServiceReference::idInvalid ? ((eServiceReferenceDVB&)ref).getServiceID().get() : -1 )
		,onid( ref.type != eServiceReference::idInvalid ? ((eServiceReferenceDVB&)ref).getOriginalNetworkID().get() : -1 )
		,tsid( ref.type != eServiceReference::idInvalid ? ((eServiceReferenceDVB&)ref).getTransportStreamID().get() : -1 )
		,dvbnamespace( ref.type != eServiceReference::idInvalid ? ((eServiceReferenceDVB&)ref).getDVBNamespace().get() : -1 )
	{
	}
	uniqueEPGKey()
		:sid(-1), onid(-1), tsid(-1), dvbnamespace(-1)
	{
	}
	uniqueEPGKey( int sid, int onid, int tsid, int dvbnamespace )
		:sid(sid), onid(onid), tsid(tsid), dvbnamespace(dvbnamespace)
	{
	}
	uniqueEPGKey( const eDVBChannelID &chid)
		:sid(-1), onid(chid.original_network_id.get()), tsid(chid.transport_stream_id.get()), dvbnamespace(chid.dvbnamespace.get())
	{
	}

	bool operator<(const uniqueEPGKey &a) const
	{
		return dvbnamespace < a.dvbnamespace || (dvbnamespace == a.dvbnamespace
		    && (onid < a.onid || (onid == a.onid
		    && (tsid < a.tsid || (tsid == a.tsid
		    && sid < a.sid)))));
	}
	bool operator==(const uniqueEPGKey &a) const
	{
		return dvbnamespace == a.dvbnamespace
		    && onid == a.onid
		    && tsid == a.tsid
		    && sid == a.sid;
	}
	operator bool() const
	{
		return sid != -1
		    || onid != -1
		    || tsid != -1
		    || dvbnamespace != -1;
	}
	struct equal
	{
		bool operator()(const uniqueEPGKey &a, const uniqueEPGKey &b) const
		{
			return a == b;
		}
	};
};

//eventMap is sorted by event_id
#define eventMap std::map<__u16, cacheData*>
//timeMap is sorted by beginTime
#define timeMap std::map<time_t, cacheData*>

#define channelMapIterator std::map<iDVBChannel*, channel_data*>::iterator
#define updateMap std::map<eDVBChannelID, time_t>

struct hash_uniqueEPGKey
{
	inline size_t operator()( const uniqueEPGKey &x) const
	{
		return (x.onid << 16) | x.tsid;
	}
};

#define tidMap std::set<__u32>
#if defined(__GXX_EXPERIMENTAL_CXX0X__)
	#define eventCache std::unordered_map<uniqueEPGKey, std::pair<eventMap, timeMap>, hash_uniqueEPGKey, uniqueEPGKey::equal>
	#ifdef ENABLE_PRIVATE_EPG
		#define contentTimeMap std::unordered_map<time_t, std::pair<time_t, __u16> >
		#define contentMap std::unordered_map<int, contentTimeMap >
		#define contentMaps std::unordered_map<uniqueEPGKey, contentMap, hash_uniqueEPGKey, uniqueEPGKey::equal >
	#endif
	#define pulledDBDataMap std::unordered_map<uniqueEPGKey, std::pair<std::set<uint32_t>, std::set<uint32_t> >, hash_uniqueEPGKey, uniqueEPGKey::equal>
#elif __GNUC_PREREQ(3,1)
	#define eventCache __gnu_cxx::hash_map<uniqueEPGKey, std::pair<eventMap, timeMap>, hash_uniqueEPGKey, uniqueEPGKey::equal>
	#ifdef ENABLE_PRIVATE_EPG
		#define contentTimeMap __gnu_cxx::hash_map<time_t, std::pair<time_t, __u16> >
		#define contentMap __gnu_cxx::hash_map<int, contentTimeMap >
		#define contentMaps __gnu_cxx::hash_map<uniqueEPGKey, contentMap, hash_uniqueEPGKey, uniqueEPGKey::equal >
	#endif
	#define pulledDBDataMap __gnu_cxx::hash_map<uniqueEPGKey, std::pair<std::set<int>, std::set<int> >, hash_uniqueEPGKey, uniqueEPGKey::equal>
#else // for older gcc use following
	#define eventCache std::hash_map<uniqueEPGKey, std::pair<eventMap, timeMap>, hash_uniqueEPGKey, uniqueEPGKey::equal >
	#ifdef ENABLE_PRIVATE_EPG
		#define contentTimeMap std::hash_map<time_t, std::pair<time_t, __u16> >
		#define contentMap std::hash_map<int, contentTimeMap >
		#define contentMaps std::hash_map<uniqueEPGKey, contentMap, hash_uniqueEPGKey, uniqueEPGKey::equal>
	#endif
	#define pulledDBDataMap std::hash_map<uniqueEPGKey, std::pair<std::set<int>, std::set<int> >, hash_uniqueEPGKey, uniqueEPGKey::equal>
#endif

#define descriptorPair std::pair<int,__u8*>
#define descriptorMap std::map<__u32, descriptorPair >

class eEPGCache;

class EPGDBThread: public eMainloop_native, private eThread, public Object
{
	struct Message
	{
		enum { unknown, process_data, shutdown, lock_service, unlock_service, unlock_first, cleanup_outdated };
		Message()
			:type(unknown)
		{
		}
		Message(const Message &msg)
			:type(msg.type), data(msg.data), service(msg.service), source(msg.source)
		{
		}
		Message(const struct uniqueEPGKey &service, int type)
			:type(type), service(service)
		{
		}
		Message(const struct uniqueEPGKey &service, const __u8 *data, int source)
			:type(process_data), data(data), service(service), source(source)
		{
		}
		Message(int type)
			:type(type)
		{
		}
		int type;
		const __u8 *data;
		struct uniqueEPGKey service;
		int source;
	};
	eEPGCache *m_epg_cache;

	pthread_mutex_t m_mutex;
	pthread_cond_t m_cond;
	std::queue<struct Message> m_queue;
	int m_running;

	std::map<uniqueEPGKey, int> m_locked_services;

	void gotMessage(const Message &message);
	void thread();
public:
	EPGDBThread(eEPGCache *cache);
	/* this functions are called from main thread */
	void sendData(const uniqueEPGKey &service, const __u8 *data, int source);
	void lockService(const uniqueEPGKey &service);
	void unlockService(const uniqueEPGKey &service, bool first=false);
	void shutdown();
	void start();
	void cleanupOutdated();
};

#endif

class cachestate
{
public:
	int state;
	uint16_t tsid;
	uint16_t onid;
	uint32_t dvbnamespace;
	int seconds; /* deferred seconds */
	enum { started, stopped, aborted, deferred, load_finished, save_finished };
	~cachestate()
	{
	}
#ifdef SWIG
private:
#endif
	cachestate(const cachestate &s)
		:state(s.state), tsid(s.tsid), onid(s.onid), dvbnamespace(s.dvbnamespace), seconds(s.seconds)
	{
	}
	cachestate(int state)
		:state(state)
	{
	}
	cachestate(int state, const uniqueEPGKey &chid, int seconds=0)
		:state(state), tsid(chid.tsid), onid(chid.onid), dvbnamespace(chid.dvbnamespace), seconds(seconds)
	{
	}
};

class eEPGCache: public iObject, public eMainloop_native, private eThread, public Object
{
	E_DECLARE_PRIVATE(eEPGCache)

#ifndef SWIG
	DECLARE_REF(eEPGCache)
	struct channel_data: public sigc::trackable
	{
		pthread_mutex_t channel_active;
		channel_data(eEPGCache*);
		eEPGCache *cache;
		ePtr<eTimer> abortTimer, zapTimer;
		int prevChannelState;
		int state;
		__u8 isRunning, haveData;
		ePtr<eDVBChannel> channel;
		ePtr<eConnection> m_stateChangedConn, m_NowNextConn, m_ScheduleConn, m_ScheduleOtherConn, m_ViasatConn;
		ePtr<iDVBSectionReader> m_NowNextReader, m_ScheduleReader, m_ScheduleOtherReader, m_ViasatReader;
		tidMap seenSections[4], calcedSections[4];
		std::set<uniqueEPGKey> m_seen_services;
		std::set<uniqueEPGKey> m_skipped_services;
#ifdef ENABLE_PRIVATE_EPG
		ePtr<eTimer> startPrivateTimer;
		int m_PrevVersion;
		int m_PrivatePid;
		uniqueEPGKey m_PrivateService;
		ePtr<eConnection> m_PrivateConn;
		ePtr<iDVBSectionReader> m_PrivateReader;
		std::set<__u8> seenPrivateSections;
		void readPrivateData(const __u8 *data, int len);
		void startPrivateReader();
#endif
#ifdef ENABLE_MHW_EPG
		std::vector<mhw_channel_name_t> m_channels;
		std::map<__u8, mhw_theme_name_t> m_themes;
		std::map<__u32, mhw_title_t> m_titles;
		std::multimap<__u32, __u32> m_program_ids;
		ePtr<eConnection> m_MHWConn, m_MHWConn2;
		ePtr<iDVBSectionReader> m_MHWReader, m_MHWReader2;
		eDVBSectionFilterMask m_MHWFilterMask, m_MHWFilterMask2;
		ePtr<eTimer> m_MHWTimeoutTimer;
		__u16 m_mhw2_channel_pid, m_mhw2_title_pid, m_mhw2_summary_pid;
		bool m_MHWTimeoutet;
		void MHWTimeout() { m_MHWTimeoutet=true; }
		void readMHWData(const __u8 *data);
		void readMHWData2(const __u8 *data);
		void startMHWReader(__u16 pid, __u8 tid);
		void startMHWReader2(__u16 pid, __u8 tid, int ext=-1);
		void startTimeout(int msek);
		bool checkTimeout() { return m_MHWTimeoutet; }
		void cleanup();
		__u8 *delimitName( __u8 *in, __u8 *out, int len_in );
		void timeMHW2DVB( u_char hours, u_char minutes, u_char *return_time);
		void timeMHW2DVB( int minutes, u_char *return_time);
		void timeMHW2DVB( u_char day, u_char hours, u_char minutes, u_char *return_time);
		void storeTitle(std::map<__u32, mhw_title_t>::iterator itTitle, const std::string &sumText, const __u8 *data);
#endif
		void readData(const __u8 *data, int len);
		void readDataViasat(const __u8 *data, int len);
		void startChannel();
		void startEPG();
		bool finishEPG();
		void abortEPG();
		void abortNonAvail();
		bool isCaching();
	};
	bool FixOverlapping(std::pair<eventMap,timeMap> &servicemap, time_t TM, int duration, const timeMap::iterator &tm_it, const uniqueEPGKey &service);
public:
	enum {PRIVATE=0, NOWNEXT=1, SCHEDULE=2, SCHEDULE_OTHER=4
#ifdef ENABLE_MHW_EPG
	,MHW=8
#endif
	,VIASAT=16
	};
	struct Message
	{
		enum
		{
			flush,
			startChannel,
			leaveChannel,
			load,
			save,
			cacheStarted,
			cacheStopped,
			cacheDeferred,
			quit,
			got_private_pid,
			got_mhw2_channel_pid,
			got_mhw2_title_pid,
			got_mhw2_summary_pid,
			loadFinished,
			saveFinished
		};
		int type;
		iDVBChannel *channel;
		uniqueEPGKey service;
		union {
			int err;
			time_t time;
			bool avail;
			int pid;
		};
		Message()
			:type(0), time(0) {}
		Message(int type)
			:type(type) {}
		Message(int type, bool b)
			:type(type), avail(b) {}
		Message(int type, iDVBChannel *channel)
			:type(type), channel(channel) {}
		Message(int type, iDVBChannel *channel, int err);
		Message(int type, const eServiceReference& service, int err=0)
			:type(type), service(service), err(err) {}
		Message(int type, time_t time)
			:type(type), time(time) {}
	};
	eFixedMessagePump<Message> messages;
	eFixedMessagePump<Message> thread_messages;
private:
	friend struct channel_data;
	friend class EPGDBThread;
	static eEPGCache *instance;

	ePtr<eTimer> cleanTimer;
	ePtr<eTimer> stopTransaktionTimer;

//	ePtr<eTimer> flushToDBTimer;
	std::map<iDVBChannel*, channel_data*> m_knownChannels;
	ePtr<eConnection> m_chanAddedConn;

	eventCache eventDB;
	updateMap channelLastUpdated;
	pulledDBDataMap pulledData;
	static pthread_mutex_t cache_lock, channel_map_lock;

#ifdef ENABLE_PRIVATE_EPG
	contentMaps content_time_tables;
#endif

	int m_running;
	int m_outdated_epg_timespan;
	char m_filename[1024];

	EPGDBThread m_db_thread;
	__u8 *m_next_section_buffer;

// called from epgcache thread
	void loadInternal();
	void saveInternal(bool do_cleanup=false);

	void thread();  // thread function
	bool copyDatabase(void *memorydb, char* filename, bool save, bool do_cleanup=false);
#ifdef ENABLE_PRIVATE_EPG
	void privateSectionRead(const uniqueEPGKey &, const __u8 *);
#endif
	void sectionRead(const __u8 *data, int source, channel_data *channel);
	bool hasExternalData(const uniqueEPGKey &service);
	void gotMessage(const Message &message);
	void flushEPG(const uniqueEPGKey & s=uniqueEPGKey());

// called from db thread
	void processData(const struct uniqueEPGKey &service, const __u8 *data, int source);
	void pushToDB(const uniqueEPGKey &);
	void pullFromDB(const uniqueEPGKey &);
	void cleanupAfterPullPush();
	void cleanupOutdated();

// called from main thread
	void cleanupOutdatedTimer();
	void timeUpdated();
	void DVBChannelAdded(eDVBChannel*);
	void DVBChannelStateChanged(iDVBChannel*);
	void DVBChannelRunning(iDVBChannel *);

// just for internal use to query events in temporary cache maps (when cache is running)
	RESULT startTimeQueryTemp(const eServiceReference &service, time_t begin=-1, int minutes=-1);
	RESULT lookupEventIdTemp(const eServiceReference &service, int event_id, const cacheData *&);
	RESULT lookupEventTimeTemp(const eServiceReference &service, time_t, const cacheData *&, int direction=0);

	__u8 *allocateSectionBuffer();

	timeMap::iterator m_timemap_cursor, m_timemap_end;
	int currentQueryTsidOnid; // needed for getNextTimeEntry.. only valid until next startTimeQuery call
#else
	eEPGCache();
	~eEPGCache();
#endif // SWIG
public:
	static eEPGCache *getInstance() { return instance; }
	static uint32_t getStringHash(std::string text);
#ifndef SWIG
	eEPGCache();
	~eEPGCache();

#ifdef ENABLE_PRIVATE_EPG
	void PMTready(eDVBServicePMTHandler *pmthandler);
#else
	void PMTready(eDVBServicePMTHandler *pmthandler) {}
#endif

#endif
	void load();
	void save();

	void applyDbBugfix20161008();
	void createUpdateTriggers();

	// must be called once!
	void setCacheFile(const char *filename);
	void setCacheTimespan(int days);
	void setOutdatedEPGTimespan(int hours);

	// called from main thread
	inline void Lock();
	inline void Unlock();

	enum {
		SIMILAR_BROADCASTINGS_SEARCH,
		EXAKT_TITLE_SEARCH,
		PARTIAL_TITLE_SEARCH,
		PARTIAL_DESCRIPTION_SEARCH
	};
	enum {
		CASE_CHECK,
		NO_CASE_CHECK
	};
	PyObject *lookupEvent(SWIG_PYOBJECT(ePyObject) list);
	PyObject *search(SWIG_PYOBJECT(ePyObject));

	// eServiceEvent are parsed epg events.. it's safe to use them after cache unlock
	// for use from python ( members: m_start_time, m_duration, m_short_description, m_extended_description )
	SWIG_VOID(RESULT) lookupEventId(const eServiceReference &service, int event_id, ePtr<eServiceEvent> &SWIG_OUTPUT);
	SWIG_VOID(RESULT) lookupEventTime(const eServiceReference &service, time_t, ePtr<eServiceEvent> &SWIG_OUTPUT, int direction=0);

	eSignal1<void, boost::any> cacheState; // sent when data collecting has started/stopped/aborted/deferred
};

#ifndef SWIG
inline void eEPGCache::Lock()
{
	pthread_mutex_lock(&cache_lock);
}

inline void eEPGCache::Unlock()
{
	pthread_mutex_unlock(&cache_lock);
}
#endif

#endif
