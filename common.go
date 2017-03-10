package main

import (
	"bufio"
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"os/user"
	"regexp"
	"strings"
	"unicode/utf16"

	"github.com/Shopify/sarama"
)

type connectionArgs struct {
	version    string
	tls        bool
	clientCert string
}

var (
	v820  = sarama.V0_8_2_0
	v821  = sarama.V0_8_2_1
	v822  = sarama.V0_8_2_2
	v900  = sarama.V0_9_0_0
	v901  = sarama.V0_9_0_1
	v1000 = sarama.V0_10_0_0

	invalidClientIDCharactersRegExp = regexp.MustCompile(`[^a-zA-Z0-9_-]`)
)

func kafkaVersion(s string) sarama.KafkaVersion {
	switch s {
	case "v0.8.2.0":
		return sarama.V0_8_2_0
	case "v0.8.2.1":
		return sarama.V0_8_2_1
	case "v0.8.2.2":
		return sarama.V0_8_2_2
	case "v0.9.0.0":
		return sarama.V0_9_0_0
	case "v0.9.0.1":
		return sarama.V0_9_0_1
	default:
		return sarama.V0_10_0_0
	}
}

func failf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func readStdinLines(max int, out chan string) {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, max), max)

	for scanner.Scan() {
		out <- scanner.Text()
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scanning input failed err=%v\n", err)
	}
	close(out)
}

// hashCode imitates the behavior of the JDK's String#hashCode method.
// https://docs.oracle.com/javase/7/docs/api/java/lang/String.html#hashCode()
//
// As strings are encoded in utf16 on the JVM, this implementation checks wether
// s contains non-bmp runes and uses utf16 surrogate pairs for those.
func hashCode(s string) (hc int32) {
	for _, r := range s {
		r1, r2 := utf16.EncodeRune(r)
		if r1 == 0xfffd && r1 == r2 {
			hc = hc*31 + r
		} else {
			hc = (hc*31+r1)*31 + r2
		}
	}
	return
}

func kafkaAbs(i int32) int32 {
	switch {
	case i == -2147483648: // Integer.MIN_VALUE
		return 0
	case i < 0:
		return i * -1
	default:
		return i
	}
}

func hashCodePartition(key string, partitions int32) int32 {
	if partitions <= 0 {
		return -1
	}

	return kafkaAbs(hashCode(key)) % partitions
}

func sanitizeUsername(u string) string {
	// Windows user may have format "DOMAIN|MACHINE\username", remove domain/machine if present
	s := strings.Split(u, "\\")
	u = s[len(s)-1]
	// Windows account can contain spaces or other special characters not supported
	// in client ID. Keep the bare minimum and ditch the rest.
	return invalidClientIDCharactersRegExp.ReplaceAllString(u, "")
}

func parseConnectionFlags(flags *flag.FlagSet, args *connectionArgs) {
	flags.StringVar(&args.version, "version", "", "Kafka protocol version")
	flags.BoolVar(&args.tls, "tls", false, "Enable TLS")
	flags.StringVar(&args.clientCert, "clientCert", "", "Path to client certificate")
}

func saramaConfig(args *connectionArgs) *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = kafkaVersion(args.version)
	usr, err := user.Current()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-consume-" + sanitizeUsername(usr.Username)
	if args.tls {
		cfg.Net.TLS.Enable = true
	}

	if args.clientCert != "" {
		cfg.Net.TLS.Config = makeTLSConfig(args.clientCert)
	}

	return cfg
}

func makeTLSConfig(path string) *tls.Config {
	cert, err := tls.LoadX509KeyPair(path, path)
	if err != nil {
		failf("Unable to load client certificate", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}
}
