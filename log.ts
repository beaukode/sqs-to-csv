export default class Log {
  public static log(...args: any[]) {
    console.log(new Date().toISOString() + ">", ...args);
  }

  public static error(...args: any[]) {
    console.error(new Date().toISOString() + ">", "ERROR:", ...args);
  }
}
