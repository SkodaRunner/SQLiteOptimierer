using System;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;

namespace IniRW
{
    class IniFile   // revision 11
    {
        readonly string Path;
        private readonly string EXE = Assembly.GetExecutingAssembly().GetName().Name ?? string.Empty;

        public string EXE1 => EXE;

        [DllImport("kernel32", CharSet = CharSet.Unicode)]
        static extern long WritePrivateProfileString(string Section, string Key, string Value, string FilePath);

        [DllImport("kernel32", CharSet = CharSet.Unicode)]
        static extern int GetPrivateProfileString(string Section, string Key, string Default, StringBuilder RetVal, int Size, string FilePath);

        [DllImport("kernel32.dll", CharSet = CharSet.Auto)]
        private static extern uint GetPrivateProfileSection(string lpAppName, IntPtr lpReturnedString, uint nSize, string lpFileName);

        public IniFile(string? IniPath = null)
        {
            Path = new FileInfo(IniPath ?? EXE + ".ini").FullName.ToString();
        }

        public string Read(string Key, string? Section = null)
        {
            var RetVal = new StringBuilder(255);
            int result=0;
            try
            {
                 result = GetPrivateProfileString(Section ?? EXE1, Key, "", RetVal, 255, Path);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Fehler beim Lesen des Schlüssels '{Key}' aus der INI-Datei '{Path}'.", ex);
            }
            if (result == 0)
            {
                int errorCode = Marshal.GetLastWin32Error();
                throw new InvalidOperationException($"Fehler beim Lesen des Schlüssels '{Key}' aus der INI-Datei '{Path}'. Fehlercode: {errorCode}");
            }

            return RetVal.ToString();
        }


        public void Write(string Key, string? Value, string? Section = null)
        {
            if (Key == null)
            {
                throw new ArgumentNullException(nameof(Key), "Key darf nicht null sein.");
            }

            if (Value == null)
            {
                throw new ArgumentNullException(nameof(Value), "Value darf nicht null sein.");
            }

            long v = WritePrivateProfileString(Section ?? EXE1,
                                               Key: Key,
                                               Value: Value,
                                               FilePath: Path);
        }

        public void DeleteKey(string Key, string? Section = null)
        {
            Write(Key, null, Section ?? EXE1);
        }

        public void DeleteSection(string? Section = null)
        {
            Write(string.Empty, string.Empty, Section ?? EXE1);
        }

        public bool KeyExists(string Key, string? Section = null)
        {
            return Read(Key, Section).Length > 0;
        }

        public string[] ReadIniAllKeys(string section)
        {
            UInt32 MAX_BUFFER = 32767;

            string[] items = new string[0];

            IntPtr pReturnedString = Marshal.AllocCoTaskMem((int)MAX_BUFFER * sizeof(char));

            UInt32 bytesReturned = GetPrivateProfileSection(section, pReturnedString, MAX_BUFFER, Path);

            if (!(bytesReturned == MAX_BUFFER - 2) || (bytesReturned == 0))
            {
                string? returnedString = Marshal.PtrToStringAuto(pReturnedString, (int)bytesReturned) ?? string.Empty;

                items = returnedString.Split(new char[] { '\0' }, StringSplitOptions.RemoveEmptyEntries);
            }

            Marshal.FreeCoTaskMem(pReturnedString);

            return items;
        }
    }
}