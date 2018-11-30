#include "BCMergeFlip.h"

template <class K, class V>
BCMergeFlip<K,V>::BCMergeFlip(std::string dir, std::string old_name, size_t old_bytes, int toflip_fd) {
	if ( old_name == "" ) {
		m_old_fd = -1;
	} else {
		m_old_fd = open((dir+"/"+old_name).c_str(), O_RDONLY, S_IRUSR|S_IWUSR);
	}
	//m_toflip_fd = open((dir+"/"+toflip_name).c_str(), O_RDONLY, S_IRUSR|S_IWUSR);
	m_toflip_fd = toflip_fd;

	if ( m_old_fd < 0 ) {
		mp_old_reader = NULL;
	} else {
		mp_old_reader = new SortReduceUtils::FileKvReader<V,K>(m_old_fd, old_bytes);
		cur_buffered_val = mp_old_reader->Next();
	}
	mp_toflip_reader = new SortReduceUtils::FileKvReader<K,V>(m_toflip_fd);
}

/*
template <class K, class V>
BCMergeFlip<K,V>::BCMergeFlip(std::string dir, std::string old_name, size_t old_bytes, std::string toflip_name, size_t toflip_bytes ) {
	m_old_fd = open((dir+"/"+old_name).c_str(), O_RDONLY, S_IRUSR|S_IWUSR);
	m_toflip_fd = open((dir+"/"+toflip_name).c_str(), O_RDONLY, S_IRUSR|S_IWUSR);

	if ( m_old_fd < 0 ) {
		mp_old_reader = NULL;
	} else {
		mp_old_reader = new SortReduceUtils::FileKvReader<V,K>(m_old_fd, old_bytes);
		cur_buffered_val = mp_old_reader->Next();
	}
	mp_toflip_reader = new SortReduceUtils::FileKvReader<K,V>(m_toflip_fd, toflip_bytes);
}

template <class K, class V>
BCMergeFlip<K,V>::BCMergeFlip(std::string dir, std::string old_name, size_t old_bytes, SortReduce<K,V>* sr_result ) {
	m_old_fd = open((dir+"/"+old_name).c_str(), O_RDONLY, S_IRUSR|S_IWUSR);
	//m_toflip_fd = open((dir+"/"+toflip_name).c_str(), O_RDONLY, S_IRUSR|S_IWUSR);

	if ( m_old_fd < 0 ) {
		mp_old_reader = NULL;
	} else {
		mp_old_reader = new SortReduceUtils::FileKvReader<V,K>(m_old_fd, old_bytes);
		cur_buffered_val = mp_old_reader->Next();
	}
	//mp_toflip_reader = new SortReduceUtils::FileKvReader<K,V>(m_toflip_fd, toflip_bytes);
	mp_sr_result = sr_result;
	cur_toflip_val = mp_sr_result->Next();
}
*/

template <class K, class V>
BCMergeFlip<K,V>::~BCMergeFlip() {
	delete mp_toflip_reader;
	if ( mp_old_reader != NULL ) {
		delete mp_old_reader;
	}
}

template <class K, class V>
inline std::tuple<V,K,bool> 
BCMergeFlip<K,V>::Next() {
	if ( mp_old_reader != NULL ) {
		if ( std::get<2>(cur_buffered_val) ) {
			std::tuple<K,V, bool> cv = mp_toflip_reader->Next(false);

			if (std::get<2>(cv) == false ) {
				std::tuple<V,K, bool> rold = cur_buffered_val;
				cur_buffered_val = mp_old_reader->Next();

				return rold;
			} else if (std::get<0>(cv) == std::get<0>(cur_buffered_val)) {
				std::tuple<V,K,bool> rold = cur_buffered_val;

				// commented so that cur_buffered_val also gets returned next call
				//cur_buffered_val = mp_old_reader->Next();

				// get the data again, advancing the pointer this time
				cv = mp_toflip_reader->Next(true);
				//cur_toflip_val = mp_sr_result->Next();
				return std::make_tuple(std::get<1>(cv), std::get<1>(rold), true);
			} else if ( std::get<0>(cv) > std::get<0>(cur_buffered_val) ) { // old first
				std::tuple<V,K, bool> rold = cur_buffered_val;
				cur_buffered_val = mp_old_reader->Next();

				return rold;
			} else { // toflip first
				// get the data again, advancing the pointer this time
				std::tuple<K,V, bool> tf = mp_toflip_reader->Next();
				return std::make_tuple(std::get<1>(tf),1,std::get<2>(tf));
			}
		} else {
			std::tuple<K,V, bool> tf = mp_toflip_reader->Next();
			return std::make_tuple(std::get<1>(tf),1,std::get<2>(tf));
		}
	} else {
		std::tuple<K,V, bool> tf = mp_toflip_reader->Next();
		if ( std::get<2>(tf) ) {
			V val = std::get<1>(tf);
			return std::make_tuple(val,1,true);
		} else {
			return std::make_tuple(0,0,false);
		}
	}
}

/*
template <class K, class V>
inline std::tuple<V,K,bool> 
BCMergeFlip<K,V>::Next() {
	std::tuple<V,K, bool> ret = std::make_tuple(0,0,false);

	if ( mp_old_reader != NULL ) {
		if ( std::get<2>(cur_buffered_val) ) {
			std::tuple<K,V, bool> cv = cur_toflip_val;
			//std::tuple<K,V, bool> ret = mp_toflip_reader->Next(false);

			if (std::get<2>(cv) == false ) {
				std::tuple<V,K, bool> rold = cur_buffered_val;
				cur_buffered_val = mp_old_reader->Next();

				return rold;
			} else if (std::get<0>(cv) == std::get<0>(cur_buffered_val)) {
				std::tuple<V,K,bool> rold = cur_buffered_val;

				// commented so that cur_buffered_val also gets returned next call
				//cur_buffered_val = mp_old_reader->Next();

				//cur_toflip_val = mp_toflip_reader->Next(); // get the data again, advancing the pointer this time
				cur_toflip_val = mp_sr_result->Next();
				return std::make_tuple(std::get<1>(ret), std::get<1>(rold), true);
			} else if ( std::get<0>(cv) > std::get<1>(cur_buffered_val) ) { // old first
				std::tuple<V,K, bool> rold = cur_buffered_val;
				cur_buffered_val = mp_old_reader->Next();

				return rold;
			} else { // toflip first
				//std::tuple<K,V, bool> tf = mp_toflip_reader->Next();
				//return std::make_tuple(std::get<1>(tf),1,std::get<2>(tf));

				//cur_toflip_val = mp_toflip_reader->Next(); // get the data again, advancing the pointer this time
				cur_toflip_val = mp_sr_result->Next();
				return std::make_tuple(std::get<1>(cv),1,std::get<2>(cv));
			}
		} else {
			std::tuple<K,V, bool> tf = cur_toflip_val;
			cur_toflip_val = mp_sr_result->Next();
			return std::make_tuple(std::get<1>(tf),1,std::get<2>(tf));
		}
	} else {
		std::tuple<K,V, bool> tf = cur_toflip_val;
		cur_toflip_val = mp_sr_result->Next();
		if ( std::get<2>(tf) ) {
			V val = std::get<1>(tf);
			return std::make_tuple(val,1,true);
		} else {
			return std::make_tuple(0,0,false);
		}
	}

	return ret;
}
*/

TEMPLATE_EXPLICIT_INSTANTIATION(BCMergeFlip)
