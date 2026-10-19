module github.com/SMSFlow-ZA/whatomate-smsflow

go 1.26.0

require github.com/shridarpatil/whatomate v0.0.0-20261019075203-7df84a224874

// Until the channel SDK is accepted upstream, standalone builds use its public
// corresponding-source fork. A Whatomate host supplies this module itself.
replace github.com/shridarpatil/whatomate => github.com/SMSFlow-ZA/whatomate v0.0.0-20261019075203-7df84a224874
