"use server";
import "server-only";

export async function getRequest(url) {
  try {
    const res = await fetch(`${process.env.PONTOON_API_ENDPOINT}${url}`);

    if (!res.ok) {
      const error = new Error(
        "An error occurred while fetching the data: " + url
      );
      console.log(res);
      // Attach extra info to the error object.
      error.info = await res.json();
      error.status = res.status;
      throw error;
    }

    return res.json();
  } catch (err) {
    console.log(err);
    throw new Error(`Error message from get request: ${err.message}`);
  }
}

export async function postRequest(url, { arg }) {
  try {
    const res = await fetch(`${process.env.PONTOON_API_ENDPOINT}${url}`, {
      method: "POST",
      headers: {
        accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(arg),
    });

    if (!res.ok) {
      const error = new Error(
        "An error occurred while fetching the data: " + url
      );
      // Attach extra info to the error object.
      error.info = await res.json();
      error.status = res.status;
      throw error;
    }

    return res.json();
  } catch (err) {
    console.log(err);
    throw new Error(`Error message from post request: ${err.message}`);
  }
}

export async function putRequest(url, { arg }) {
  try {
    const res = await fetch(`${process.env.PONTOON_API_ENDPOINT}${url}`, {
      method: "PUT",
      headers: {
        accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(arg),
    });

    if (!res.ok) {
      const error = new Error(
        "An error occurred while fetching the data: (" + res.status + ") " + url
      );
      // Attach extra info to the error object.
      error.info = await res.json();
      error.status = res.status;
      throw error;
    }

    return res.json();
  } catch (err) {
    console.log(err);
    throw new Error(`Error message from puts request: ${err.message}`);
  }
}

export async function deleteRequest(url) {
  try {
    const res = await fetch(`${process.env.PONTOON_API_ENDPOINT}${url}`, {
      method: "DELETE",
    });

    if (!res.ok) {
      const error = new Error("An error occurred while fetching the data.");
      // Attach extra info to the error object.
      error.info = await res.json();
      error.status = res.status;
      throw error;
    }

    return res.json();
  } catch (err) {
    console.log(err);
    throw new Error(`Error message from delete request: ${err.message}`);
  }
}

export async function pollTaskStatus(url, interval = 3000, timeout = 180000) {
  const startTime = Date.now();

  while (true) {
    try {
      const response = await getRequest(url);

      // Check if the task is complete
      if (response.status == "COMPLETE") {
        return response.output;
      }

      // Check if timeout exceeded
      if (Date.now() - startTime > timeout) {
        throw new Error("Polling timed out");
      }
    } catch (error) {
      console.error("Polling error:", error);
    }

    // Wait before the next attempt
    await new Promise((resolve) => setTimeout(resolve, interval));
  }
}

export async function rerunTransferRequest(key, { arg }) {
  return postRequest(`/transfers/${arg}/rerun`, { arg: {} });
}

export async function runDestinationRequest(key, { arg }) {
  const { destinationId, scheduleOverride } = arg;

  // Prepare the request body based on the schedule override
  let requestBody = {};
  if (scheduleOverride && scheduleOverride.backfillType) {
    requestBody.type = scheduleOverride.backfillType;

    if (
      scheduleOverride.backfillType === "INCREMENTAL" &&
      scheduleOverride.startTime &&
      scheduleOverride.endTime
    ) {
      // startTime and endTime are already ISO strings from the frontend
      requestBody.start = scheduleOverride.startTime;
      requestBody.end = scheduleOverride.endTime;
    }
  }

  return postRequest(`/destinations/${destinationId}/run`, {
    arg: requestBody,
  });
}
