export const getVendorTypeDisplayText = (sourceType) => {
  const displayNames = {
    snowflake: "Snowflake",
    bigquery: "BigQuery",
    redshift: "Redshift",
    postgresql: "Postgres",
    memory: "Memory",
    console: "Console",
  };
  const name = displayNames[sourceType];
  if (name === undefined) {
    console.log(
      `sourceType: ${sourceType} is not valid. Could not get a display name`
    );
  }
  return name;
};

export const getScheduleText = (schedule) => {
  // Helper function to format the time in local time
  function formatTime(date) {
    const options = { hour: "numeric", minute: "2-digit", hour12: true };
    return date.toLocaleTimeString("en-US", options);
  }

  let nowUTC = new Date();
  let nextRunTime = new Date(
    Date.UTC(
      nowUTC.getUTCFullYear(),
      nowUTC.getUTCMonth(),
      nowUTC.getUTCDate(),
      nowUTC.getUTCHours(),
      nowUTC.getUTCMinutes(),
      0
    )
  );

  const weekDays = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
  ];

  switch (schedule.frequency) {
    case "DAILY":
      nextRunTime.setUTCDate(nextRunTime.getUTCDate() + 1);
      nextRunTime.setUTCHours(schedule.hour || 0, schedule.minute || 0, 0);
      break;

    case "WEEKLY":
      const currentUTCDay = nowUTC.getUTCDay();
      const daysUntilNextRun = (schedule.day - currentUTCDay + 7) % 7;
      nextRunTime.setUTCDate(nextRunTime.getUTCDate() + daysUntilNextRun);
      nextRunTime.setUTCHours(schedule.hour || 0, schedule.minute || 0, 0);
      break;

    case "HOURLY":
      nextRunTime.setUTCHours(nextRunTime.getUTCHours() + 1);
      nextRunTime.setUTCMinutes(schedule.minute || 0);
      break;

    case "SIXHOURLY":
      nextRunTime.setUTCHours(nextRunTime.getUTCHours() + 6);
      nextRunTime.setUTCMinutes(schedule.minute || 0);
      break;

    default:
      return "Destination has an invalid schedule";
  }

  // Convert to local time for display
  const localRunTime = new Date(nextRunTime);

  let nextRunString = `Runs ${schedule.frequency.toLowerCase()} at ${formatTime(
    localRunTime
  )}`;
  if (schedule.frequency === "WEEKLY") {
    nextRunString = `Runs weekly on ${weekDays[schedule.day]} at ${formatTime(
      localRunTime
    )}`;
  }
  if (schedule.frequency === "HOURLY") {
    nextRunString = `Runs hourly ${
      !schedule.minute || schedule.minute == 0
        ? "on the hour"
        : "at :" + schedule.minute + " past the hour"
    }`;
  }
  if (schedule.frequency === "6HOURLY") {
    nextRunString = `Runs every 6 hours at :${
      !schedule.minute || schedule.minute == 0 ? "00" : schedule.minute
    } past the hour`;
  }

  return nextRunString;
};

export const getNextRunTime = (schedule) => {
  // Set default values for day, hour, and minute
  const day = schedule.day ?? 0; // Default to Sunday if not provided
  const hour = schedule.hour ?? 0; // Default to 0 if not provided
  const minute = schedule.minute ?? 0; // Default to 0 if not provided

  const now = new Date();
  const nowUTC = new Date(
    Date.UTC(
      now.getUTCFullYear(),
      now.getUTCMonth(),
      now.getUTCDate(),
      now.getUTCHours(),
      now.getUTCMinutes(),
      now.getUTCSeconds(),
      now.getUTCMilliseconds()
    )
  );

  // Create a new Date object for the scheduled time in UTC
  let nextRun = new Date(
    Date.UTC(
      nowUTC.getUTCFullYear(),
      nowUTC.getUTCMonth(),
      nowUTC.getUTCDate(),
      hour, // Use UTC hour
      minute, // Use UTC minute
      0, // Set seconds to 0
      0 // Set milliseconds to 0
    )
  );

  // Handle different frequencies
  switch (schedule.frequency) {
    case "DAILY":
      nextRun.setUTCDate(nextRun.getUTCDate() + 1);
      break;
    case "WEEKLY":
      nextRun.setUTCDate(nextRun.getUTCDate() + 7);
      break;
    case "HOURLY":
      nextRun.setUTCHours(nextRun.getUTCHours() + 1);
      break;
    case "SIXHOURLY":
      nextRun.setUTCHours(nextRun.getUTCHours() + 6);
      break;
    default:
      break;
  }

  // Check if the current time has already passed today's scheduled time
  if (nowUTC > nextRun) {
    switch (schedule.frequency) {
      case "DAILY":
        nextRun.setUTCDate(nextRun.getUTCDate() + 1); // next day
        break;
      case "WEEKLY":
        nextRun.setUTCDate(nextRun.getUTCDate() + 7); // next week
        break;
      case "HOURLY":
        nextRun.setUTCHours(nextRun.getUTCHours() + 1); // next hour
        break;
      case "SIXHOURLY":
        nextRun.setUTCHours(nextRun.getUTCHours() + 6); // next 6 hours
        break;
      default:
        break;
    }
  }

  // If the schedule has a specific day defined (for weekly frequency)
  if (schedule.frequency === "WEEKLY" && day !== undefined) {
    const currentDayOfWeek = nowUTC.getUTCDay();
    const daysUntilNextRun = (day - currentDayOfWeek + 7) % 7; // calculate how many days until the next scheduled day
    nextRun.setUTCDate(nowUTC.getUTCDate() + daysUntilNextRun);
  }

  return nextRun.toISOString(); // Always returns UTC time
};
