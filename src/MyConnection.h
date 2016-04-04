#ifndef __RMYSQL_MY_CONNECTION__
#define __RMYSQL_MY_CONNECTION__

#include <Rcpp.h>
#include <mysql.h>
#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>

const char* as_api_string(const Rcpp::Nullable<Rcpp::CharacterVector>& cpp_string,
                          const char* arg);

class MyResult;

// convenience typedef for shared_ptr to PqConnection
class MyConnection;
typedef boost::shared_ptr<MyConnection> MyConnectionPtr;

class MyConnection : boost::noncopyable {
  MYSQL* pConn_;
  MyResult* pCurrentResult_;

public:

  MyConnection(const Rcpp::Nullable<Rcpp::CharacterVector>& host,
               const Rcpp::Nullable<Rcpp::CharacterVector>& user,
               const Rcpp::Nullable<Rcpp::CharacterVector>& password,
               const Rcpp::Nullable<Rcpp::CharacterVector>& db,
               unsigned int port,
               const Rcpp::Nullable<Rcpp::CharacterVector>& unix_socket,
               unsigned long client_flag,
               const Rcpp::Nullable<Rcpp::CharacterVector>& groups,
               const Rcpp::Nullable<Rcpp::CharacterVector>& default_file,
               const Rcpp::Nullable<Rcpp::CharacterVector>& ssl_key,
               const Rcpp::Nullable<Rcpp::CharacterVector>& ssl_cert,
               const Rcpp::Nullable<Rcpp::CharacterVector>& ssl_ca,
               const Rcpp::Nullable<Rcpp::CharacterVector>& ssl_capath,
               const Rcpp::Nullable<Rcpp::CharacterVector>& ssl_cipher) :
    pCurrentResult_(NULL)
  {
    pConn_ = mysql_init(NULL);
    // Enable LOCAL INFILE for fast data ingest
    mysql_options(pConn_, MYSQL_OPT_LOCAL_INFILE, 0);
    // Default to UTF-8
    mysql_options(pConn_, MYSQL_SET_CHARSET_NAME, "UTF8");

    if (!groups.isNull())
      mysql_options(pConn_, MYSQL_READ_DEFAULT_GROUP, as_api_string(groups, "groups"));

    if (!default_file.isNull())
      mysql_options(pConn_, MYSQL_READ_DEFAULT_FILE,
                    as_api_string(default_file, "default_file"));

    if (!ssl_key.isNull() || !ssl_cert.isNull() || !ssl_ca.isNull() ||
        !ssl_capath.isNull() || !ssl_cipher.isNull()) {
      mysql_ssl_set(
        pConn_,
        as_api_string(ssl_key, "ssl_key"),
        as_api_string(ssl_cert, "ssl_cert"),
        as_api_string(ssl_ca, "ssl_ca"),
        as_api_string(ssl_capath, "ssl_capath"),
        as_api_string(ssl_cipher, "ssl_cipher")
      );
    }

    if (!mysql_real_connect(pConn_,
        as_api_string(host, "host"),
        as_api_string(user, "user"),
        as_api_string(password, "password"),
        as_api_string(db, "db"),
        port,
        as_api_string(unix_socket, "unix_socket"),
        client_flag)) {
      mysql_close(pConn_);
      Rcpp::stop("Failed to connect: %s", mysql_error(pConn_));
    }
  }

  Rcpp::List connectionInfo() {
    return Rcpp::List::create(
      Rcpp::_["host"] = std::string(mysql_get_host_info(pConn_)),
      Rcpp::_["server"] = std::string(mysql_get_server_info(pConn_)),
      Rcpp::_["client"] = std::string(mysql_get_client_info())
    );
  }

  std::string quoteString(std::string input) {
    // Create buffer with enough room to escape every character
    std::string output;
    output.resize(input.size() * 2 + 1);

    size_t end = mysql_real_escape_string(pConn_, &output[0],
      input.data(), input.size());
    output.resize(end);

    return output;
  }

  MYSQL* conn() {
    return pConn_;
  }

  // Cancels previous query, if needed.
  void setCurrentResult(MyResult* pResult);
  bool isCurrentResult(MyResult* pResult) {
    return pCurrentResult_ == pResult;
  }
  bool hasQuery() {
    return pCurrentResult_ != NULL;
  }

  bool exec(std::string sql) {
    setCurrentResult(NULL);

    if (mysql_real_query(pConn_, sql.data(), sql.size()) != 0)
      Rcpp::stop(mysql_error(pConn_));

    MYSQL_RES* res = mysql_store_result(pConn_);
    if (res != NULL)
      mysql_free_result(res);

    return true;
  }

  ~MyConnection() {
    try {
      mysql_close(pConn_);
    } catch(...) {};
  }

};

#endif
