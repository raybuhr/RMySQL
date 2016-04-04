#include "MyConnection.h"
#include "MyResult.h"

class concat_string {
public:
  template<class T>
  concat_string& operator<< (const T& arg) {
    m_stream << arg;
    return *this;
  }
  operator std::string() const {
    return m_stream.str();
  }
protected:
  std::ostringstream m_stream;
};

const char* as_api_string_nonnull(const Rcpp::CharacterVector& cpp_string,
                                  const char* arg);
const char* as_api_string(const Rcpp::Nullable<Rcpp::CharacterVector>& cpp_string,
                          const char* arg) {
  if (cpp_string.isNull())
    return NULL;

  return as_api_string_nonnull(cpp_string.get(), arg);
}

const char* as_api_string_nonnull(const Rcpp::CharacterVector& cpp_string,
                                  const char* arg) {
  if (cpp_string.length() != 1) {
    Rcpp::stop(concat_string() << "Expecting a single value for " <<
      arg << " argument");
  }

  return CHAR(cpp_string[0]);
}

void MyConnection::setCurrentResult(MyResult* pResult) {
  if (pResult == pCurrentResult_)
    return;

  if (pCurrentResult_ != NULL) {
    if (pResult != NULL)
      Rcpp::warning("Cancelling previous query");

    pCurrentResult_->close();
  }
  pCurrentResult_ = pResult;
}
