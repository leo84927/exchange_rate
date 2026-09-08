package handle

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

/*
 * 為了 defer 的效果而抽取成獨立 func
 * 不寫成獨立 func 的話，defer 會寫在 for 迴圈內，導致每次迴圈都要等到整個迴圈結束才關閉 body，會造成連線資源被占住。
 */
func fetch(ctx context.Context, client *http.Client, url, headerKey, headerValue string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set(headerKey, headerValue)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	// 兩個供應商成功時都只回 200，其餘一律當整批失敗；認證失敗（401）也走這裡
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("responded %s: %s", resp.Status, body)
	}

	return body, nil
}
