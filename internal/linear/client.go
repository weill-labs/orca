package linear

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

const defaultEndpoint = "https://api.linear.app/graphql"

var ErrMissingAPIKey = errors.New("LINEAR_API_KEY not set")

const (
	issueLookupQuery = `query($id: String!) {
		issue(id: $id) {
			team { key }
			state { id name }
		}
	}`
	workflowStatesQuery = `query($teamKey: String!) {
		workflowStates(filter: { team: { key: { eq: $teamKey } } }) {
			nodes { id name }
		}
	}`
	issueUpdateMutation = `mutation($id: String!, $stateId: String!) {
		issueUpdate(id: $id, input: { stateId: $stateId }) {
			success
		}
	}`
)

type Options struct {
	APIKey     string
	Endpoint   string
	HTTPClient *http.Client
}

type Client struct {
	apiKey     string
	endpoint   string
	httpClient *http.Client
}

func New(opts Options) (*Client, error) {
	apiKey := strings.TrimSpace(opts.APIKey)
	if apiKey == "" {
		var err error
		apiKey, err = APIKeyFromEnv()
		if err != nil {
			return nil, err
		}
	}
	if apiKey == "" {
		return nil, ErrMissingAPIKey
	}

	endpoint := strings.TrimSpace(opts.Endpoint)
	if endpoint == "" {
		endpoint = defaultEndpoint
	}

	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	return &Client{
		apiKey:     apiKey,
		endpoint:   endpoint,
		httpClient: httpClient,
	}, nil
}

func APIKeyFromEnv() (string, error) {
	if key := strings.TrimSpace(os.Getenv("LINEAR_API_KEY")); key != "" {
		return key, nil
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}

	data, err := os.ReadFile(filepath.Join(homeDir, ".env"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", fmt.Errorf("read ~/.env: %w", err)
	}

	const prefix = "export LINEAR_API_KEY="
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		value := strings.TrimSpace(strings.TrimPrefix(line, prefix))
		value = strings.Trim(value, `"'`)
		if value != "" {
			return value, nil
		}
	}

	return "", nil
}

func (c *Client) SetIssueStatus(ctx context.Context, issue, targetState string) error {
	issue = strings.TrimSpace(issue)
	targetState = strings.TrimSpace(targetState)
	if issue == "" {
		return fmt.Errorf("issue is required")
	}
	if targetState == "" {
		return fmt.Errorf("state is required")
	}

	metadata, err := c.lookupIssue(ctx, issue)
	if err != nil {
		return err
	}
	if metadata.State.Name == targetState {
		return nil
	}

	stateID, err := c.lookupStateID(ctx, metadata.Team.Key, targetState)
	if err != nil {
		return err
	}
	if stateID == "" {
		return fmt.Errorf("workflow state %q not found for team %q", targetState, metadata.Team.Key)
	}
	if metadata.State.ID == stateID {
		return nil
	}

	return c.updateIssueState(ctx, issue, stateID)
}

type issueMetadata struct {
	Team struct {
		Key string `json:"key"`
	} `json:"team"`
	State struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"state"`
}

func (c *Client) lookupIssue(ctx context.Context, issue string) (issueMetadata, error) {
	var response struct {
		Issue issueMetadata `json:"issue"`
	}
	err := c.graphQL(ctx, graphQLRequest{
		Query:     issueLookupQuery,
		Variables: map[string]string{"id": issue},
	}, &response)
	if err != nil {
		return issueMetadata{}, fmt.Errorf("lookup issue %s: %w", issue, err)
	}
	if response.Issue.Team.Key == "" {
		return issueMetadata{}, fmt.Errorf("lookup issue %s: missing team key", issue)
	}
	if response.Issue.State.ID == "" {
		return issueMetadata{}, fmt.Errorf("lookup issue %s: missing current state", issue)
	}
	return response.Issue, nil
}

func (c *Client) lookupStateID(ctx context.Context, teamKey, targetState string) (string, error) {
	var response struct {
		WorkflowStates struct {
			Nodes []struct {
				ID   string `json:"id"`
				Name string `json:"name"`
			} `json:"nodes"`
		} `json:"workflowStates"`
	}
	err := c.graphQL(ctx, graphQLRequest{
		Query:     workflowStatesQuery,
		Variables: map[string]string{"teamKey": teamKey},
	}, &response)
	if err != nil {
		return "", fmt.Errorf("lookup workflow states for team %s: %w", teamKey, err)
	}

	for _, state := range response.WorkflowStates.Nodes {
		if state.Name == targetState {
			return state.ID, nil
		}
	}
	return "", nil
}

func (c *Client) updateIssueState(ctx context.Context, issue, stateID string) error {
	var response struct {
		IssueUpdate struct {
			Success bool `json:"success"`
		} `json:"issueUpdate"`
	}
	err := c.graphQL(ctx, graphQLRequest{
		Query: issueUpdateMutation,
		Variables: map[string]string{
			"id":      issue,
			"stateId": stateID,
		},
	}, &response)
	if err != nil {
		return fmt.Errorf("update issue %s state: %w", issue, err)
	}
	if !response.IssueUpdate.Success {
		return fmt.Errorf("update issue %s state: mutation reported failure", issue)
	}
	return nil
}

type graphQLRequest struct {
	Query     string `json:"query"`
	Variables any    `json:"variables,omitempty"`
}

type graphQLError struct {
	Message    string `json:"message"`
	Extensions struct {
		Code string `json:"code"`
	} `json:"extensions"`
}

func (c *Client) graphQL(ctx context.Context, request graphQLRequest, out any) error {
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", c.apiKey)

	response, err := c.httpClient.Do(httpRequest)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		message := strings.TrimSpace(string(responseBody))
		if message == "" {
			message = response.Status
		}
		return fmt.Errorf("unexpected status %s: %s", response.Status, message)
	}

	var envelope struct {
		Data   json.RawMessage `json:"data"`
		Errors []graphQLError  `json:"errors"`
	}
	if err := json.Unmarshal(responseBody, &envelope); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if len(envelope.Errors) > 0 {
		return formatGraphQLErrors(envelope.Errors)
	}
	if out == nil || len(envelope.Data) == 0 {
		return nil
	}
	if err := json.Unmarshal(envelope.Data, out); err != nil {
		return fmt.Errorf("decode data: %w", err)
	}
	return nil
}

func formatGraphQLErrors(graphQLErrors []graphQLError) error {
	messages := make([]string, 0, len(graphQLErrors))
	for _, gqlErr := range graphQLErrors {
		message := gqlErr.Message
		if code := strings.TrimSpace(gqlErr.Extensions.Code); code != "" {
			message = fmt.Sprintf("%s (%s)", message, code)
		}
		messages = append(messages, message)
	}
	return errorsJoin(messages)
}

func errorsJoin(messages []string) error {
	return errors.New(strings.Join(messages, "; "))
}
