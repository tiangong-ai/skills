# Environment Variables

This skill reads these environment variables:

- `UNSTRUCTURED_AUTH_TOKEN`: FastAPI bearer token for `Authorization: Bearer <token>`.
- `UNSTRUCTURED_PROVIDER` (optional): MinerU vision provider override, for example `openai`, `gemini`, `vllm`.
- `UNSTRUCTURED_MODEL` (optional): Model name override for the selected provider, for example `Qwen/Qwen3.5-122B-A10B-FP8`.
- `UNSTRUCTURED_API_BASE_URL`: API base URL, for example `https://your-unstructured-host:7770`.

Example:

```bash
export UNSTRUCTURED_AUTH_TOKEN="your-fastapi-bearer-token"
export UNSTRUCTURED_API_BASE_URL="https://your-unstructured-host:7770"
# Optional routing overrides. Omit them to let the service choose defaults.
export UNSTRUCTURED_PROVIDER="vllm"
export UNSTRUCTURED_MODEL="Qwen/Qwen3.5-122B-A10B-FP8"
```
