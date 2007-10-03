#include <windows.h>
#include <process.h>
#include <errno.h>
#include <tchar.h>

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#define DllExport __declspec(dllexport)


static int al_sleep(lua_State* L)
{
  DWORD v = (DWORD)(luaL_checkint(L, 1) * 1000);
  Sleep(v);
  return 0;
}

static int al_platform(lua_State* L)
{
  lua_pushstring(L, "windows");
  return 1;
}

static int al_execute(lua_State *L)
{
    int i, top;
	STARTUPINFO si;
    PROCESS_INFORMATION pi;
	luaL_Buffer buf;
	LPTSTR cmdline;

	top = lua_gettop(L);
	luaL_buffinit(L, &buf);

	// Concatenate the 
	luaL_addstring(&buf, luaL_checkstring(L, 1));
	for (i = 2; i <= top; i++) {
		int j;
		const char *str = luaL_checkstring(L, i);
		int len = lua_objlen(L, i);
		char hasspace = 0;
		for (j = 0; j < len; j++) {
			if (str[j] == ' ' || str[j] == '\t') {
				hasspace = 1;
				break;
			}
		}
		luaL_addstring(&buf, " ");
		if (hasspace) luaL_addstring(&buf, "\"");
		luaL_addstring(&buf, luaL_checkstring(L, i));
		if (hasspace) luaL_addstring(&buf, "\"");
	}
	luaL_pushresult(&buf);
	cmdline = (LPTSTR)lua_tostring(L, -1);

    ZeroMemory(&si, sizeof(si));
    ZeroMemory(&pi, sizeof(pi));
	si.cb = sizeof(si);

    // Start the child process. 
    if( !CreateProcess(NULL,   // No module name (use command line)
        cmdline,               // Command line
        NULL,                  // Process handle not inheritable
        NULL,                  // Thread handle not inheritable
        FALSE,                 // Set handle inheritance to FALSE
        DETACHED_PROCESS,      // No creation flags
        NULL,                  // Use parent's environment block
        NULL,                  // Use parent's starting directory 
        &si,                   // Pointer to STARTUPINFO structure
        &pi)                   // Pointer to PROCESS_INFORMATION structure
    ) 
    {
        // Error
		lua_pushboolean(L, 0);
		return 1;
    }
    CloseHandle(pi.hProcess);
    CloseHandle(pi.hThread);
	lua_pushboolean(L, 1);
	return 1;
}

struct luaL_Reg funcs[] = {
	{"sleep",    al_sleep},
	{"execute",  al_execute},
	{"platform", al_platform},
	{NULL, NULL}
};

DllExport int luaopen_alua_core(lua_State *L)
{
	luaL_register(L, "alua.core", funcs);
	return 1;
}
