using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace Opereta.Controllers
{

    public class AccountController : BaseController
    {
        public IActionResult Login()
        {
            return View();
        }
    }
}