# Home Index Transcribe

Home Index Transcribe is an RPC module for [Home Index](https://github.com/nashspence/home-index). It uses [WhisperX](https://github.com/m-bain/whisperX) to generate transcripts and subtitle files from audio and video. The module implements the RPC interface described in the Home Index project so that transcripts can be indexed and searched.

## Quick start

The `docker-compose.yml` in this repository launches a small stack containing Home Index, Meilisearch and this transcribe module. After installing Docker, run:

```bash
docker compose up
```

Drop media files into `bind-mounts/files/` and Home Index will automatically invoke the module to produce transcripts. Metadata and cache files are stored under `bind-mounts/`.

## Configuration

Several environment variables control how WhisperX runs:

- `NAME` – module name (`transcribe` by default)
- `DEVICE` – compute device (`cuda` or `cpu`)
- `WHISPER_MODEL` – Whisper model variant (e.g. `medium`)
- `BATCH_SIZE` – batch size for transcription
- `COMPUTE_TYPE` – computation precision (`int8` by default)
- `LANGUAGE` – language code (`en` by default)
- `PYANNOTE_DIARIZATION_AUTH_TOKEN` – token for speaker diarization

### Running manually

If you want to start the RPC server without Docker:

```bash
pip install -r requirements.txt
python packages/home_index_transcribe/main.py
```

The server listens on `0.0.0.0:9000`. Add its endpoint to the `MODULES` environment variable of Home Index so it is called during indexing.

## More information

See the [Home Index documentation](https://github.com/nashspence/home-index) for details on the RPC module specification and additional configuration options.
