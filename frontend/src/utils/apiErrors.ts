import axios from "axios";

/** Says what actually went wrong, so a failed request is never mistaken for an empty result or a dead API. */
export function describeApiError(error: unknown): string {
  if (axios.isAxiosError(error)) {
    const status = error.response?.status;
    if (status === undefined) return "API unreachable";
    if (status === 503) return "Search is temporarily unavailable. Try again shortly.";
    if (status === 400) return "That query could not be parsed. Check its syntax and try again.";
    return `The API returned an error (HTTP ${status}).`;
  }
  return error instanceof Error ? error.message : "Unable to load data";
}
