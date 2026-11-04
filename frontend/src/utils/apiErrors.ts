import axios from "axios";

/** Says what actually went wrong, so a failed request is never mistaken for an empty result or a dead API. */
export function describeApiError(error: unknown): string {
  if (axios.isAxiosError(error)) {
    const status = error.response?.status;
    if (status === undefined) return "API unreachable";
    // The API's own message says what failed (search, a dependency, a bad query); never guess which.
    const detail = typeof error.response?.data?.detail === "string" ? error.response.data.detail : null;
    if (status === 503) return `${detail ?? "The service is temporarily unavailable"}. Try again shortly.`;
    if (status === 400) return `${detail ?? "The request could not be processed"}. Check it and try again.`;
    return `The API returned an error (HTTP ${status}).`;
  }
  return error instanceof Error ? error.message : "Unable to load data";
}
