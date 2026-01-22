package github_app

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/bradleyfalzon/ghinstallation/v2"
	"github.com/google/go-github/v69/github"

	"github.com/argoproj/argo-cd/v3/applicationset/services/github_app_auth"
	appsetutils "github.com/argoproj/argo-cd/v3/applicationset/utils"
)

type githubInstallationClientCacheRegistry struct {
	storages map[string]*ghinstallation.Transport
	lock     *sync.RWMutex
}

var globalInstallationClientCache = &githubInstallationClientCacheRegistry{
	storages: make(map[string]*ghinstallation.Transport),
	lock:     &sync.RWMutex{},
}

func (r *githubInstallationClientCacheRegistry) get(g github_app_auth.Authentication) (*ghinstallation.Transport, bool) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	key := fmt.Sprintf("%d/%d", g.Id, g.InstallationId)
	client, exists := r.storages[key]
	return client, exists
}

func (r *githubInstallationClientCacheRegistry) put(g github_app_auth.Authentication, client *ghinstallation.Transport) {
	r.lock.Lock()
	defer r.lock.Unlock()
	key := fmt.Sprintf("%d/%d", g.Id, g.InstallationId)
	r.storages[key] = client
}

func getOptionalHTTPClientAndTransport(optionalHTTPClient ...*http.Client) (*http.Client, http.RoundTripper) {
	httpClient := appsetutils.GetOptionalHTTPClient(optionalHTTPClient...)
	if len(optionalHTTPClient) > 0 && optionalHTTPClient[0] != nil && optionalHTTPClient[0].Transport != nil {
		// will either use the provided custom httpClient and it's transport
		return httpClient, optionalHTTPClient[0].Transport
	}
	// or the default httpClient and transport
	return httpClient, http.DefaultTransport
}

// Client builds a github client for the given app authentication.
func Client(g github_app_auth.Authentication, url string, optionalHTTPClient ...*http.Client) (*github.Client, error) {
	httpClient, transport := getOptionalHTTPClientAndTransport(optionalHTTPClient...)

	var itr *ghinstallation.Transport
	var err error
	if cachedInstallationClient, exists := globalInstallationClientCache.get(g); exists {
		itr = cachedInstallationClient
	} else {
		rt, err := ghinstallation.New(transport, g.Id, g.InstallationId, []byte(g.PrivateKey))
		if err != nil {
			return nil, fmt.Errorf("failed to create github app install: %w", err)
		}
		itr = rt
		globalInstallationClientCache.put(g, itr)
	}

	if url == "" {
		url = g.EnterpriseBaseURL
	}
	var client *github.Client
	httpClient.Transport = itr
	if url == "" {
		client = github.NewClient(httpClient)
	} else {
		itr.BaseURL = url
		client, err = github.NewClient(httpClient).WithEnterpriseURLs(url, url)
		if err != nil {
			return nil, fmt.Errorf("failed to create github enterprise client: %w", err)
		}
	}
	return client, nil
}
