package common

import (
	"flag"
	"os"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	DefaultString = "__DEFAULT_STRING"
	DefaultInt    = 0
)

type ViperConfig struct {
}

func NewViperConfig() *ViperConfig {
	appEnvironment := strings.ToLower(os.Getenv("APP_ENVIRONMENT"))
	if appEnvironment == "" {
		appEnvironment = "dev"
	}

	viper.SetConfigName("config." + appEnvironment)
	viper.SetConfigType("yaml")

	viper.AddConfigPath("/app/")
	viper.AddConfigPath("./config")

	if appEnvironment == "test" {
		viper.AddConfigPath("../../config")
	}

	viper.ReadInConfig()
	viper.AutomaticEnv()

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()

	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.BindPFlags(pflag.CommandLine)

	return &ViperConfig{}
}

func (v *ViperConfig) getDomainKey(domain string, key string) string {
	if domain != "" {
		return domain + "." + key
	} else {
		return key
	}
}

func (v *ViperConfig) GetString(domain string, key string) string {
	value := viper.GetString(key)
	if value == DefaultString {
		value = viper.GetString(v.getDomainKey(domain, key))
	}

	if value == DefaultString {
		return ""
	}

	return value
}

func (v *ViperConfig) GetInt64(domain string, key string) int64 {
	value := viper.GetInt64(key)
	if value == DefaultInt {
		value = viper.GetInt64(v.getDomainKey(domain, key))
	}

	return value
}

func (v *ViperConfig) GetInt(domain string, key string) int {
	value := viper.GetInt(key)
	if value == DefaultInt {
		value = viper.GetInt(v.getDomainKey(domain, key))
	}

	return value
}

func (v *ViperConfig) GetBool(domain string, key string) bool {
	value := viper.GetBool(key)
	if value == false {
		return viper.GetBool(v.getDomainKey(domain, key))
	} else {
		return value
	}
}
