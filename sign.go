package queueservice

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Requests are signed with HTTP Message Signatures (RFC 9421), HMAC-SHA256
// keyed with the secret access key, so the secret itself is never sent:
//
//	Content-Digest: sha-256=:<base64 SHA-256 of the body>:        (RFC 9530)
//	Signature-Input: th=("@method" "@authority" "@path" "@query" "content-digest");created=1767225600;keyid="<access key id>";alg="hmac-sha256"
//	Signature: th=:<base64 HMAC-SHA256 of the signature base>:
//
// content-digest is covered whenever the request has a body, which ties the
// body to the signature.

// contentDigest is the Content-Digest value of data.
func contentDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha-256=:" + base64.StdEncoding.EncodeToString(sum[:]) + ":"
}

// sigAuthority is the @authority component: the host, lower case, without
// a default port.
func sigAuthority(req *http.Request) string {
	host := req.Host
	if host == "" {
		host = req.URL.Host
	}
	host = strings.ToLower(host)
	return strings.TrimSuffix(strings.TrimSuffix(host, ":443"), ":80")
}

// signatureBase builds the signed bytes (RFC 9421 section 2.5) of req for
// the given Signature-Input member value (after "th=").
func signatureBase(req *http.Request, params string, digest bool) string {
	path := req.URL.EscapedPath()
	if path == "" {
		path = "/"
	}
	var b strings.Builder
	b.WriteString(`"@method": ` + req.Method + "\n")
	b.WriteString(`"@authority": ` + sigAuthority(req) + "\n")
	b.WriteString(`"@path": ` + path + "\n")
	b.WriteString(`"@query": ?` + req.URL.RawQuery + "\n")
	if digest {
		b.WriteString(`"content-digest": ` + req.Header.Get("Content-Digest") + "\n")
	}
	b.WriteString(`"@signature-params": ` + params)
	return b.String()
}

// signRequest adds Signature-Input and Signature to req. It covers
// content-digest when req carries a Content-Digest header (set it first
// for every request with a body).
func signRequest(req *http.Request, keyID, secret string, now time.Time) {
	components := `"@method" "@authority" "@path" "@query"`
	digest := req.Header.Get("Content-Digest") != ""
	if digest {
		components += ` "content-digest"`
	}
	params := "(" + components + ");created=" + strconv.FormatInt(now.Unix(), 10) +
		`;keyid="` + sfEscape(keyID) + `";alg="hmac-sha256"`
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signatureBase(req, params, digest)))
	req.Header.Set("Signature-Input", "th="+params)
	req.Header.Set("Signature", "th=:"+base64.StdEncoding.EncodeToString(mac.Sum(nil))+":")
}

// sfEscape escapes a Structured Fields string's contents (RFC 8941).
func sfEscape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s)
}
