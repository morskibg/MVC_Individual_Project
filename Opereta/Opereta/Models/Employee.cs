using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Identity;

namespace Opereta.Models
{
    public class Employee : IdentityUser
    {
        [Required]
        public string FirstName { get; set; }

        [Required]
        public string LastName { get; set; }
        public int Age { get; set; }

        [Required]
        public string PersonalNumber { get; set; }

        public string Gender { get; set; }

        public string Rank { get; set; }

        public DateTime HireDate { get; set; }
        public DateTime? ReleaseDate { get; set; }

    }
}
